SELECT status, COUNT(discount) AS discount_count, MAX(amount) AS max_amount FROM orders GROUP BY status ORDER BY status DESC
