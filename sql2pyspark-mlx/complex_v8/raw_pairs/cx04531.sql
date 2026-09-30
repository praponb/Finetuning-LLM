WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh > 48.2
      AND quality IN ('OK', 'BAD')
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_clean,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM base s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(reading_quality), 1, 4) AS reading_quality_prefix,
        ABS(reading_voltage - 227.1) AS reading_voltage_distance,
        COALESCE(reading_voltage, 0.0) AS reading_voltage_filled
    FROM exploded_detail_readings
)
SELECT
    site_id,
    quality,
    reading_quality,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(reading_voltage_filled) / NULLIF(SUM(reading_voltage_distance), 0), 3) AS reading_voltage_filled_to_reading_voltage_distan,
    MIN(reading_voltage_filled) AS min_reading_voltage_filled,
    COUNT(*) AS cnt
FROM tagged
GROUP BY GROUPING SETS ((site_id, quality, reading_quality), (site_id), (quality), ())
