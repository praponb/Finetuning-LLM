SELECT status, MAX(discount) - MIN(discount) discount_range FROM orders GROUP BY status
