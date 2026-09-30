WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 12
      AND kwh >= 15.9
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_or_unknown,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM prepared s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.quality = 'OK'
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM line_level s
),
line_level_2 AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.quality_or_unknown,
        s.reading_id,
        s.reading_voltage,
        s.last_reading_read_at,
        m.id AS reading_id_2,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage_2,
        m.quality AS reading_quality_2,
        m.read_at AS reading_read_at
    FROM with_counts s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    COALESCE(quality_or_unknown, 'ALL') AS quality_or_unknown,
    COALESCE(reading_quality_2, 'ALL') AS reading_quality_2,
    ROUND(SUM(kwh) / NULLIF(SUM(reading_kwh), 0), 3) AS kwh_to_reading_kwh_ratio
FROM line_level_2
GROUP BY GROUPING SETS ((quality_or_unknown, reading_quality_2), (quality_or_unknown), ())
