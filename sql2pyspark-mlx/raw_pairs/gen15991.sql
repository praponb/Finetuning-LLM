select * from (select status, plan, monthly_fee from subscriptions) pivot (count(monthly_fee) for plan in ('standard', 'enterprise', 'premium', 'basic'))
