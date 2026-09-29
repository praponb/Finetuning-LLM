SELECT status, COUNT(shipping_fee) AS shipping_fee_count
FROM orders
WHERE store_id = 15
GROUP BY status
HAVING SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) < 31
ORDER BY status, shipping_fee_count DESC
LIMIT 5
