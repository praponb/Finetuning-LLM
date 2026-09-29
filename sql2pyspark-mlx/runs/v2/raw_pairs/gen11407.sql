SELECT store_id, COALESCE(amount, 0) AS amount_filled, COALESCE(amount, 0) AS amount_filled_2
FROM orders
WHERE shipping_fee >= 11;
