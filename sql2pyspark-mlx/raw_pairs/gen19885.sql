SELECT
    CASE WHEN carrier IN ('Kerry', 'UPS') THEN 'priority' ELSE 'other' END AS carrier_group,
    CASE WHEN cost >= 100 THEN 'high' WHEN cost >= 35 THEN 'mid' ELSE 'low' END AS cost_bucket,
    COUNT(*) AS total_count,
    MAX(shipped_date) AS last_shipped_date
FROM shipments
WHERE status = 'RETURNED' AND (order_id IS NOT NULL OR order_id IS NOT NULL)
GROUP BY carrier_group, cost_bucket
ORDER BY last_shipped_date DESC
