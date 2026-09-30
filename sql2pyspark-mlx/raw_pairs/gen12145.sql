SELECT id, amount, NTILE(2) OVER (ORDER BY amount DESC, id) AS amount_ntile FROM orders WHERE shipping_fee < 23;
