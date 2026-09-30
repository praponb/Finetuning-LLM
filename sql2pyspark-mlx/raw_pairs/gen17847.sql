WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        status,
        ROUND(cost, 0) AS cost_rounded
    FROM shipments
    WHERE shipped_date > DATE '2023-03-06'
      AND carrier IS DISTINCT FROM 'DHL'
)
-- Grouped metrics
SELECT
    CASE WHEN cost_rounded >= 66 THEN 'HIGH' ELSE 'LOW' END AS cost_rounded_tier,
    status,
    APPROX_PERCENTILE(cost_rounded, 0.9) AS cost_rounded_p90,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    COUNT(*) AS cnt,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    COUNT(*) AS row_count,
    RANK() OVER (PARTITION BY CASE WHEN cost_rounded >= 66 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(DISTINCT carrier) DESC) AS distinct_carriers_rank
FROM valid
GROUP BY CASE WHEN cost_rounded >= 66 THEN 'HIGH' ELSE 'LOW' END, status
ORDER BY cost_rounded_tier NULLS FIRST;
