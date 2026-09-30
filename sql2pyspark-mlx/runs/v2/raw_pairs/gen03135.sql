SELECT order_date, CAST(amount AS INT) AS amount_int
FROM orders
WHERE status IN ('RETURNED', 'CANCELLED') OR shipping_fee BETWEEN 10 AND 31;
