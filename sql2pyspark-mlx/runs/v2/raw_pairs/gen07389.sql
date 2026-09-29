select * from (select status, plan, monthly_fee from subscriptions where plan in ('basic', 'enterprise'))
pivot (count(monthly_fee) for plan in ('enterprise', 'premium', 'standard'))
