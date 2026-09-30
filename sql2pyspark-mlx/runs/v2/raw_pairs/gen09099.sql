SELECT
    order_id,
    COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count,
    ROUND((100 * SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_dhl,
    ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
WHERE status NOT IN ('DELIVERED', 'LOST')
GROUP BY 1
HAVING SUM(cost) < 150
ORDER BY order_id DESC;
