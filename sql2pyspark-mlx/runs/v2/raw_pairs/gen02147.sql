select * from (select status, plan, monthly_fee from subscriptions)
pivot (avg(monthly_fee) for plan in ('standard', 'basic', 'enterprise', 'premium'))
order by status
