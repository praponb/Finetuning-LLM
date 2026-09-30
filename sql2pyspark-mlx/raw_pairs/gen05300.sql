SELECT status, MAX(discount) - MIN(discount) AS discount_range FROM orders GROUP BY status HAVING SUM(amount) < 4200 ORDER BY status
