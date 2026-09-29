SELECT s.region, ROUND((100 * SUM(CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_cancelled, COUNT(o.amount) amount_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE o.status NOT IN ('COMPLETED', 'PENDING')
GROUP BY s.region;
