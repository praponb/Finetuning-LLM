SELECT
    CASE WHEN cost >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    COUNT(*) FILTER (WHERE status = 'IN_TRANSIT') AS in_transit_rows
FROM shipments
GROUP BY CASE WHEN cost >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', shipped_date)
