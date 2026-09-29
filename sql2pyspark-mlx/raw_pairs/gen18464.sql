SELECT
    CASE WHEN YEAR(delivered_date) < 2024 THEN 'before_2024' WHEN YEAR(delivered_date) = 2024 THEN '2024' ELSE 'after_2024' END AS delivered_cohort,
    CASE WHEN cost >= 99 THEN 'high' WHEN cost >= 21 THEN 'mid' ELSE 'low' END AS cost_bucket,
    order_id,
    COUNT(cost) AS cost_count,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count,
    COUNT(*) AS total_count
FROM shipments
WHERE carrier <> 'DHL' AND shipped_date >= '2024-10-28' AND cost <= 44
GROUP BY delivered_cohort, cost_bucket, order_id
ORDER BY cost_count, delivered_cohort, cost_bucket, order_id
LIMIT 10
