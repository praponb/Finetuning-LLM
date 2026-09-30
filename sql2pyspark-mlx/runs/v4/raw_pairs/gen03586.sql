SELECT id, plan, start_date, RANK() OVER (PARTITION BY plan ORDER BY start_date DESC) AS start_date_rank FROM subscriptions;
