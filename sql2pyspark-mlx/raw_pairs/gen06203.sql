SELECT channel, COUNT(discount) AS discount_count
FROM orders
WHERE customer_id = 16 AND (channel IN ('web', 'store') OR customer_id IN (6, 27, 32))
GROUP BY channel
HAVING COUNT(DISTINCT discount) > 5
ORDER BY channel DESC
