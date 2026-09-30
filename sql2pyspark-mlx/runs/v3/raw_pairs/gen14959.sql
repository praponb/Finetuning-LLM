SELECT shipping_fee, discount, LOWER(status) status_lower FROM orders WHERE shipping_fee BETWEEN 18 AND 35 OR customer_id IN (1, 7, 8)
