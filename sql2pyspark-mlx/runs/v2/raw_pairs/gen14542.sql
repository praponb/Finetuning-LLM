select * from (select status, plan, monthly_fee from subscriptions)
pivot (sum(monthly_fee) for plan in ('enterprise', 'basic'))
