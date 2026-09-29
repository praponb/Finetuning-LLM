WITH filtered_shipments AS (
    SELECT *, CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END AS is_ups
    FROM shipments
    WHERE shipped_date > '2025-01-01' OR status = 'IN_TRANSIT'
),
status_summary AS (
    SELECT
        status,
        CASE WHEN cost >= 73 THEN 'high' WHEN cost >= 37 THEN 'mid' ELSE 'low' END AS cost_bucket,
        ROUND(AVG(is_ups), 2) AS avg_is_ups,
        COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count
    FROM filtered_shipments
    GROUP BY status, CASE WHEN cost >= 73 THEN 'high' WHEN cost >= 37 THEN 'mid' ELSE 'low' END
)
SELECT
    status,
    cost_bucket,
    avg_is_ups,
    dhl_count
FROM status_summary
WHERE dhl_count >= 8
ORDER BY status, avg_is_ups DESC;
