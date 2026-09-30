SELECT id, store_id, amount, NTILE(10) OVER (PARTITION BY store_id ORDER BY amount, id) AS quartile FROM orders WHERE shipping_fee BETWEEN 13 AND 26
