SELECT id, customer_id FROM orders WHERE channel NOT IN ('mobile', 'store') UNION SELECT id, customer_id FROM orders WHERE amount BETWEEN 1120 AND 1560
