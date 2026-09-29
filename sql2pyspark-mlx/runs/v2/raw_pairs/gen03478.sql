select * from (select date_trunc('MONTH', start_date) as start_month, plan, monthly_fee from subscriptions) pivot (sum(monthly_fee) for plan in ('basic', 'standard', 'enterprise', 'premium'))
