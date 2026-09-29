SELECT * FROM (SELECT id, plan, start_date, customer_id, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS rnk FROM subscriptions) AS t WHERE rnk <= 3 ORDER BY start_date
