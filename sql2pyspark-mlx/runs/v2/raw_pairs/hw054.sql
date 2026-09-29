SELECT id, NULLIF(discount, 0) AS discount_nonzero FROM orders;
