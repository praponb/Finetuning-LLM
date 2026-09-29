WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date,
        CASE WHEN voltage >= 233.8 THEN 'HIGH' WHEN voltage >= 222.3 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
    WHERE quality NOT IN ('EST', 'OK')
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM filtered_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
        )
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND HOUR(m.read_at) BETWEEN 6 AND 8
              AND m.quality IS NOT NULL
        )
      AND s.voltage >= 215.7
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    quality,
    voltage_tier,
    ROUND(AVG(kwh), 2) AS avg_kwh
FROM screened
GROUP BY ROLLUP(site_id, quality, voltage_tier)
ORDER BY site_id NULLS LAST, quality NULLS LAST, voltage_tier NULLS LAST
