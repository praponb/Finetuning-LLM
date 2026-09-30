SELECT discount, customer_id, shipping_fee, FLOOR(shipping_fee) AS shipping_fee_whole FROM orders WHERE customer_id IS NULL ORDER BY shipping_fee_whole
