select * from (select status, plan, monthly_fee from subscriptions)
pivot (avg(monthly_fee) for plan in ('standard', 'enterprise', 'premium', 'basic'))
order by status;
