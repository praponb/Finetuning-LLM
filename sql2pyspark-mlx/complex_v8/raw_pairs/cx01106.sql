WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 101.2 THEN 'HIGH' WHEN kwh >= 34.6 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage > 224.8
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(quality)) AS quality_len,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM cleaned_readings
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage
    FROM derived s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IN ('EST', 'OK')
        )
      AND s.voltage <= 240.2
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    kwh_tier,
    read_at_label,
    kwh_per_voltage,
    ABS(kwh - 79.4) AS kwh_distance,
    ROUND(voltage * 0.25, 2) AS voltage_adj
FROM filtered
ORDER BY voltage DESC, site_id DESC NULLS FIRST, id
LIMIT 10
