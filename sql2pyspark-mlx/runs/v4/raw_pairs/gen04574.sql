SELECT status, COUNT(shipping_fee) AS shipping_fee_count, SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) AS web_amount
FROM orders
WHERE status = 'RETURNED'
GROUP BY status
HAVING SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) >= 78
ORDER BY web_amount, shipping_fee_count DESC
