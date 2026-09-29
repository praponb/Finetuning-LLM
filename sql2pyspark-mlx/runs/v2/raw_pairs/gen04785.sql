WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY end_date) AS rnk FROM subscriptions) SELECT id, customer_id, end_date, status FROM ranked WHERE rnk <= 3
