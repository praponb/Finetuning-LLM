WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN prior_rows_1d >= 5 THEN 'HIGH' WHEN prior_rows_1d >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS prior_rows_1d_tier
    FROM matched
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM labeled
    WHERE prior_rows_1d < (SELECT MAX(prior_rows_1d) FROM labeled)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        COALESCE(prior_voltage_1d, 0.0) AS prior_voltage_1d_filled
    FROM filtered
    WHERE prior_rows_1d_tier NOT IN ('LOW', 'HIGH')
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_voltage_1d,
    prior_rows_1d_tier,
    days_since_read_at,
    prior_voltage_1d_filled
FROM enriched
WHERE prior_rows_1d <= 6
ORDER BY read_at DESC NULLS FIRST, id
LIMIT 20
