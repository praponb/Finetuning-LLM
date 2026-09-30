SELECT status, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
FROM orders
GROUP BY status
HAVING SUM(CASE WHEN channel = 'web' THEN shipping_fee ELSE 0 END) > 64;
