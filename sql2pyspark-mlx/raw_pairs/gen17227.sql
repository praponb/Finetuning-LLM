SELECT *, DATE_TRUNC('year', order_date) AS order_date_period, CAST(amount AS INT) AS amount_int
FROM orders
WHERE store_id = 5
