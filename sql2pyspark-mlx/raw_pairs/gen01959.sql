select customer_id, sum(case when plan = 'standard' then monthly_fee else 0 end) as standard_monthly_fee, count(*) as row_count from subscriptions group by customer_id;
