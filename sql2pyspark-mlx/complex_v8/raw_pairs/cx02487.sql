WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LENGTH(TRIM(quality)) AS quality_len,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 15:00:00'
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
)
SELECT
    quality_len,
    site_id,
    MIN(read_at) AS first_read_at,
    ROUND(STDDEV(voltage), 2) AS stddev_voltage
FROM with_activity
GROUP BY ROLLUP(quality_len, site_id)
ORDER BY quality_len NULLS LAST, site_id NULLS LAST
