SELECT
    order_id,
    YEAR(delivered_date) AS delivered_year,
    AVG(cost) AS avg_cost,
    ROUND((100 * SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_delivered,
    COUNT(*) AS row_count
FROM shipments
WHERE NOT (order_id IS NOT NULL AND status = 'IN_TRANSIT' AND cost >= 71)
GROUP BY order_id, YEAR(delivered_date)
HAVING row_count >= 5
ORDER BY avg_cost DESC
