setwd("C:/Users/yashg/Desktop/Acads/Fall 2017/Quant Methods/Data/")


# Dependencies ------------------------------------------------------------
library("data.table")
library("xts")
library("reshape2")



GetNewWeights = function(PrevHoldings, Returns, backtest.date){
  #Returns = ReturnData.xts
  Returns = as.data.frame(t(Returns[paste0(backtest.date),]))
  colnames(Returns) = "RET"
  Returns[, "gvkey"] = rownames(Returns)
  Returns = as.data.table(Returns)
  
  temp = merge(PrevHoldings[, list(gvkey, weight)], Returns[, list(gvkey, RET)], 
               all.x = T, by = "gvkey", sort = F)[['RET']]
  PrevHoldings[, RET:= temp]
  
  PrevHoldings[, NewWeight:= -100]
  PrevHoldings[weight>0, NewWeight:= weight*(1+RET)]
  PrevHoldings[weight>0, NewWeight:= NewWeight/sum(NewWeight, na.rm = T)]
  
  PrevHoldings[weight<0, NewWeight:= abs(weight)*(1+RET)]
  PrevHoldings[weight<0, NewWeight:= NewWeight/sum(NewWeight, na.rm = T)]
  PrevHoldings[weight<0, NewWeight := - NewWeight]
  PrevHoldings[, weight:= NewWeight]
  return(PrevHoldings[, list(gvkey, weight, RET)])
  
}

GetSPConstituents = function(SPData, backtest.date){
  #SPData  = SP.constituents
  #backtest.date = as.Date("2009-01-01")
  
  constituents = SPData[backtest.date >= from & backtest.date <= thru, gvkey]
  return(constituents)
}

#' @param data - xts object with return stream of required tickers
#' @param latest - latest month to be used for return calculation
#' @param oldest - oldest moneth to be used for return calculation
GetMOMRank = function(data, latest, oldest, stocks, backtest.date){
  common.stocks = as.matrix(intersect(stocks, colnames(data)))
  data = data[paste0("/", backtest.date), paste(common.stocks)]
  data = data[(nrow(data)-21*oldest):(nrow(data)-(latest-1)*21),]
  cumulative.returns = as.matrix(apply(data, 2, function (x) sum(x, na.rm = T)))
  names(cumulative.returns) = common.stocks
  cumulative.returns = sort(cumulative.returns, decreasing = T)
  mom.rank = order(cumulative.returns, decreasing = T)
  names(mom.rank) = names(cumulative.returns)
  return(mom.rank)
  
}

GetWeight = function(factor.rank){
  number.long = floor(length(factor.rank)*0.2)
  long.ranks = factor.rank[1:number.long]
  short.ranks = factor.rank[(length(factor.rank)-number.long-1):length(factor.rank)]
  portfolio.ranks = c(long.ranks, short.ranks)
  
  long.weight = abs(long.ranks - mean(portfolio.ranks))
  long.weight = long.weight/sum(long.weight)
  short.weight = abs(short.ranks - mean(portfolio.ranks))
  short.weight = short.weight/sum(short.weight)
  return(c(long.weight, -short.weight))
}

GetSecurityReturn = function(Portfolio.holdings, Returns, backtest.date){
  # Portfolio.holdings = Portfolio.temp
  # Returns = ReturnData.xts
  Returns = as.data.frame(t(Returns[paste0(backtest.date),]))
  colnames(Returns) = "RET"
  Returns[, "gvkey"] = rownames(Returns)
  Returns = as.data.table(Returns)
  
  Portfolio.holdings[, RET:= merge(Portfolio.holdings, Returns[, list(gvkey, RET)], 
                                   all.x = T, by = "gvkey", sort = F)[['RET']]]
  return(Portfolio.holdings)
  
}

GetPortfolioReturn = function(Prev.holdings, backtest.date, Returns){
  Prev.holdings[, RET := GetSecurityReturn(Portfolio.holdings = Prev.holdings[, list(gvkey, weight)], 
                                           Returns = Returns, backtest.date = backtest.date)[['RET']]]
  daily.return = Prev.holdings[, sum(weight*RET, na.rm = T)]
  return(daily.return)
  
}
# Load Data ---------------------------------------------------------------
SP.constituents = fread("S&PConstituents.csv")
SP.fundamental = fread("FundamentalData-12Oct.csv")
SP.ratios = fread("FinancialRatio-12Oct.csv")
ReturnData = fread("S&PDailyData.csv")
ReturnData.2 = fread("S&PDailyData_correct.csv")

# Clean data --------------------------------------------------------------
SP.constituents[, from := as.Date(from, format = "%m/%d/%Y")]
SP.constituents[, thru := as.Date(thru, format = "%m/%d/%Y")]
SP.constituents[is.na(thru), thru := as.Date("2020/1/1")]
SP.constituents[, co_cusip := substr(co_cusip, 1, 8)]
ReturnData[, RET:=as.numeric(RET)] # there are 8581 NA's
ReturnData[, date:= as.Date(date, format = "%m/%d/%Y")]
ReturnData = ReturnData[order(date, TICKER)]
ReturnData.2[, datadate:= as.Date(datadate, format = "%m/%d/%Y")]
ReturnData.2 = ReturnData.2[order(GVKEY, datadate)]
ReturnData.2[, adj.price := prccd/ajexdi*trfd]

ReturnData.2[, diff.gvkey := c(NA,diff(GVKEY))]
ReturnData.2[['prev.price']] = c(NA, ReturnData.2[1:(nrow(ReturnData.2)-1), adj.price])

ReturnData.2 = ReturnData.2[diff.gvkey ==0 ]
ReturnData.2 = na.omit(ReturnData.2)
ReturnData.2 = ReturnData.2[, RET:=(adj.price/prev.price-1)]

SP.ratios[, public_date:= paste0(substr(public_date, 5, 6), "/", 
                                 substr(public_date, 7, 8), "/",substr(public_date, 1, 4))]
SP.ratios[, public_date:= as.Date(public_date, format = "%m/%d/%Y")]

# Some duplication issue with certain gvkeys in the daily return file
# Removing these for now, need to investigate the reason
check.gvkeys = ReturnData.2[, length(unique(LIID)), by = GVKEY]
bad.gvkeys = check.gvkeys[V1>1, GVKEY]
ReturnData.2 = ReturnData.2[!(GVKEY %in% bad.gvkeys), ]

##### Check Return data calulcation
# g = 1300
# cusip = SP.constituents[gvkey == g, unique(co_cusip)]
# return.1 = ReturnData[CUSIP==cusip, list(date,RET)]
# return.2 = ReturnData.2[GVKEY==g, list(datadate, RET)]
# plot(c(as.matrix(cumprod(1+return.1[, 2]))))
# lines(cumprod(1+ return.2[, 2]), col = "2")

# Backtest setup ----------------------------------------------------------
backtest.dates = sort(as.Date(ReturnData.2[, unique(datadate)]))
backtest.dates = backtest.dates[255:length(backtest.dates)]
rebalance.dates = backtest.dates[endpoints(backtest.dates, on = "quarters")]
ReturnData.wide = dcast(ReturnData.2, datadate ~ GVKEY, fun.aggregate = sum, value.var = "RET")
ReturnData.xts = xts(ReturnData.wide[, 2:dim(ReturnData.wide)[2]], order.by = ReturnData.wide[, "datadate"])

GetValueRank = function(ratio.data, backtest.date, stocks){
  # ratio.data = SP.ratios
  # backtest.date = backtest.dates[1]
  # stocks = FirstIndex
  
  ratio.data = ratio.data[gvkey %in% stocks & public_date < backtest.date]
  ratio.data = ratio.data[order(public_date)]
  ratio.data = ratio.data[, .SD[.N], by = "gvkey"]
  ratio.data[, rank.bm := rank(-bm)]
  ratio.data[, rank.pe := rank(pe_exi)]
  ratio.data[, rank.ps:= rank(ps)]
  ratio.data[, rank.overall:= rank(1/3*(rank.bm+rank.ps+rank.pe))]
  ratio.data = ratio.data[order(rank.overall, decreasing = F)]
  rank.output = as.integer(ratio.data[, rank.overall])
  names(rank.output) = ratio.data[, gvkey]
  return(rank.output)
}

# Initial Portfolio -------------------------------------------------------
FirstIndex = as.character(GetSPConstituents(SPData = SP.constituents, backtest.date = backtest.dates[1]))
MOM.rank = GetMOMRank(data = ReturnData.xts, latest = 2, oldest = 12, 
                      stocks = FirstIndex, backtest.date = backtest.dates[1])
Value.rank = GetValueRank(ratio.data = SP.ratios, 
                    backtest.date = backtest.dates[1], stocks = FirstIndex)
MOM.weight = GetWeight(factor.rank = MOM.rank)
Value.weight = GetWeight(Value.rank)

### Initialize the factor portfolios
MOM.PortHoldings = vector('list', length = length(backtest.dates))
names(MOM.PortHoldings) = backtest.dates
Value.PortHoldings = vector('list', length = length(backtest.dates))
names(Value.PortHoldings) = backtest.dates

### Initialize the factor temp portfolios
Portfolio.temp.mom = as.data.table(matrix(NA, length(MOM.weight), 3))
colnames(Portfolio.temp.mom) = c("gvkey", "weight", "RET")
Portfolio.temp.mom[, gvkey := names(MOM.weight)]
Portfolio.temp.mom[, weight:= MOM.weight]
Portfolio.temp.mom[, RET:= NA]
MOM.PortHoldings[[backtest.dates[1]]] = Portfolio.temp.mom

Portfolio.temp.value = as.data.table(matrix(NA, length(Value.weight), 3))
colnames(Portfolio.temp.value) = c("gvkey", "weight", "RET")
Portfolio.temp.value[, gvkey := names(Value.weight)]
Portfolio.temp.value[, weight:= Value.weight]
Portfolio.temp.value[, RET:= NA]
Value.PortHoldings[[backtest.dates[1]]] = Portfolio.temp.value
###################
Wealth = matrix(NA, length(backtest.dates), 4)
Wealth[1,] = 1
daily.return = matrix(NA, length(backtest.dates), 4)
daily.return[1,] = 0 
# Backtest ----------------------------------------------------------------
for(i in 2:length(backtest.dates)){
  Prev.holdings.mom = MOM.PortHoldings[[backtest.dates[i-1]]]
  Prev.holdings.value = Value.PortHoldings[[backtest.dates[i-1]]]
  backtest.date = backtest.dates[i]
  if(backtest.date %in% rebalance.dates){
    #### Get Updated Factor Ranks and Weights
    MOM.rank = GetMOMRank(data = ReturnData.xts, latest = 2, oldest = 12, 
                          stocks = FirstIndex, backtest.date = backtest.date)
    MOM.weight = GetWeight(MOM.rank)
    Value.rank = GetValueRank(ratio.data = SP.ratios, 
                              backtest.date = backtest.date, stocks = FirstIndex)
    Value.weight = GetWeight(Value.rank)
    
    # Create temp factor portfolios
    Portfolio.temp.mom = as.data.table(matrix(NA, length(MOM.weight), 2))
    colnames(Portfolio.temp.mom) = c("gvkey", "weight")
    Portfolio.temp.mom[, gvkey := names(MOM.weight)]
    Portfolio.temp.mom[, weight:= MOM.weight]
    Portfolio.temp.mom = GetSecurityReturn(Portfolio.holdings = Portfolio.temp.mom[, list(gvkey, weight)], 
                                       Returns = ReturnData.xts, backtest.date = backtest.date)
    
    Portfolio.temp.value = as.data.table(matrix(NA, length(Value.weight), 2))
    colnames(Portfolio.temp.value) = c("gvkey", "weight")
    Portfolio.temp.value[, gvkey := names(Value.weight)]
    Portfolio.temp.value[, weight:= Value.weight]
    Portfolio.temp.value = GetSecurityReturn(Portfolio.holdings = Portfolio.temp.value[, list(gvkey, weight)], 
                                           Returns = ReturnData.xts, backtest.date = backtest.date)
  } else{
    Portfolio.temp.mom = GetNewWeights(PrevHoldings = Prev.holdings.mom, 
                        Returns = ReturnData.xts, backtest.date = backtest.date)
    Portfolio.temp.value = GetNewWeights(PrevHoldings = Prev.holdings.value, 
                                       Returns = ReturnData.xts, backtest.date = backtest.date)
  }
  
  
  MOM.PortHoldings[[backtest.date]] = Portfolio.temp.mom
  Value.PortHoldings[[backtest.date]] = Portfolio.temp.value
  daily.return[i, 1] = GetPortfolioReturn(Prev.holdings = Prev.holdings.mom, 
                                       backtest.date = backtest.date, Returns = ReturnData.xts)
  daily.return[i, 2] = GetPortfolioReturn(Prev.holdings = Prev.holdings.value, 
                                          backtest.date = backtest.date, Returns = ReturnData.xts)
  Wealth[i, 1] = Wealth[i-1, 1]*(1 + daily.return[i, 1])
  Wealth[i, 2] = Wealth[i-1, 2]*(1 + daily.return[i, 2])
  print(i)
  
}

Wealth.xts = xts(Wealth[, 1:2], order.by = backtest.dates)
plot(Wealth.xts[,1])
lines(Wealth.xts[, 2], col ="2")
daily.return.xts = xts(daily.return, order.by = backtest.dates)


# Analysis ----------------------------------------------------------------
nstocks = matrix(NA, length(backtest.dates), 1)
for(i in 1:length(backtest.dates)){
  backtest.date = backtest.dates[i]
  port.temp = MOM.PortHoldings[[backtest.date]]
  nstocks[i] = nrow(port.temp)
}

