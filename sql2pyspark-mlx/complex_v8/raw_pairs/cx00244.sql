WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN kwh >= 106.5 THEN 'A' WHEN kwh >= 33.9 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM prepared_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND TO_DATE(m.read_at) < DATE '2025-03-08'
              AND m.kwh <= 109.0
        )
)
SELECT
    kwh_tier,
    quality,
    GROUPING(kwh_tier) AS is_total_kwh_tier,
    MIN(voltage) AS min_voltage
FROM screened
GROUP BY GROUPING SETS ((kwh_tier, quality), (kwh_tier))
