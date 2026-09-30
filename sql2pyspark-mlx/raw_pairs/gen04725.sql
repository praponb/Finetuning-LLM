SELECT store_id, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
FROM orders
WHERE status IN ('PENDING', 'CANCELLED') AND shipping_fee >= 38
GROUP BY 1
ORDER BY pct_shipped ASC, store_id ASC
LIMIT 10;
