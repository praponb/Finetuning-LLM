SELECT id, customer_id, channel, CAST(discount AS INT) AS discount_int, amount / NULLIF(discount, 0) AS amount_per_discount FROM orders WHERE customer_id IS NULL;
