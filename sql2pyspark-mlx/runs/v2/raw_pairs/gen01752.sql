SELECT *, FLOOR(shipping_fee) AS shipping_fee_whole FROM orders WHERE customer_id IS NOT NULL;
