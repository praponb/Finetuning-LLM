select customer_id, sum(case when plan = 'standard' then 1 else 0 end) as standard_count from subscriptions group by customer_id having sum(case when plan = 'standard' then 1 else 0 end) > 29;
