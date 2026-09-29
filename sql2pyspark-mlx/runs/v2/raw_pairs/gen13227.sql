SELECT id, customer_id, start_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) AS row_num FROM subscriptions) AS t WHERE row_num <= 2
