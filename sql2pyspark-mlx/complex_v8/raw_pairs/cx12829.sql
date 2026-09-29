WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        ROUND(kwh * 0.25, 2) AS kwh_adj,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 10:00:00'
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label,
        s.kwh_adj,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM scoped s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage >= 216.1
),
unique_rows AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        read_at_label,
        kwh_adj,
        reading_id,
        reading_quality,
        reading_voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY read_at DESC, id DESC, reading_id DESC) AS rn
        FROM detail_rows
    ) ranked
    WHERE rn = 1
),
with_details_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_label,
        s.kwh_adj,
        s.reading_id,
        s.reading_voltage,
        m.voltage AS reading_voltage_2,
        m.quality AS reading_quality_2
    FROM unique_rows s
    JOIN meter_readings m ON m.id = s.reading_id AND m.voltage BETWEEN 221.4 AND 238.8
)
-- Final projection
SELECT
    id,
    site_id,
    quality,
    read_at_label,
    kwh_adj,
    reading_id,
    reading_voltage,
    reading_voltage_2,
    reading_quality_2,
    read_at + INTERVAL 15 MINUTES AS read_at_plus_15m
FROM with_details_reading
ORDER BY quality, reading_voltage_2
