select status, sum(monthly_fee) as total_monthly_fee, count(case when plan = 'basic' then 1 end) as basic_count from subscriptions where plan = 'enterprise' group by 1;
