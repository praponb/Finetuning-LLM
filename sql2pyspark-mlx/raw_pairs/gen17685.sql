WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC, id) AS pos FROM subscriptions) SELECT * FROM ranked WHERE pos <= 3;
