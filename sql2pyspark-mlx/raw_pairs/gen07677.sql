WITH ranked AS (SELECT id, customer_id, monthly_fee, status, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS pos FROM subscriptions) SELECT * FROM ranked WHERE pos <= 1
