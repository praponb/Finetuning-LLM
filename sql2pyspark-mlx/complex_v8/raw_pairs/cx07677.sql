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
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(prior_voltage_1d * 1.5, 2) AS prior_voltage_1d_adj,
        CASE WHEN prior_voltage_1d >= 980.0 THEN 'A' WHEN prior_voltage_1d >= 490.0 THEN 'B' ELSE 'C' END AS prior_voltage_1d_tier
    FROM matched
    WHERE site_id <= 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, prior_rows_1d, prior_voltage_1d_tier, prior_voltage_1d_adj
    FROM tagged
) src
PIVOT (
    COUNT(prior_voltage_1d_adj)
    FOR prior_voltage_1d_tier IN ('B', 'C')
)
ORDER BY read_at, prior_rows_1d
