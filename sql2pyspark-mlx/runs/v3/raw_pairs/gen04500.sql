SELECT status, MAX(discount) - MIN(discount) AS discount_range
FROM orders
GROUP BY status
HAVING discount_range >= 21;
