SELECT *, ROUND(shipping_fee, 2) AS shipping_fee_rounded, CEIL(amount) AS amount_whole FROM orders WHERE store_id = 23 ORDER BY discount DESC, id LIMIT 10;
