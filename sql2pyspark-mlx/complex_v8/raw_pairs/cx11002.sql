WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(voltage, 0.0) AS voltage_filled,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE voltage <= 231.7
      AND kwh >= 34.6
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.days_since_read_at,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM filtered s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        CASE WHEN reading_voltage >= 238.8 THEN 'A' WHEN reading_voltage >= 219.5 THEN 'B' ELSE 'C' END AS reading_voltage_tier,
        reading_id,
        PERCENTILE_APPROX(reading_voltage, 0.5) AS median_reading_voltage,
        ROUND(STDDEV(reading_kwh), 2) AS stddev_reading_kwh,
        MIN(reading_voltage) AS min_reading_voltage
    FROM line_level_readings
    GROUP BY 1, 2, 3
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM grouped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
        )
)
SELECT site_id, reading_voltage_tier, metric, value
FROM (
    SELECT site_id, reading_voltage_tier, median_reading_voltage, min_reading_voltage
    FROM qualified
) src
UNPIVOT (
    value FOR metric IN (median_reading_voltage AS median_reading_voltage_v, min_reading_voltage AS min_reading_voltage_v)
) unp
