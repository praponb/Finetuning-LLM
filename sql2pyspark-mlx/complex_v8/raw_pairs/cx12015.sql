WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 223.3) AS voltage_distance
    FROM meter_readings
    WHERE voltage < 232.0
      AND kwh < 101.2
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(CASE WHEN quality <> 'EST' THEN 1 END) AS reading_quality_count,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM base s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality NOT IN ('EST', 'OK')
        )
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.reading_count,
        s.reading_count_2,
        si.installed_kw AS site_installed_kw
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw > 96.9
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM joined_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh > 43.1
              AND m.quality IS NOT NULL
        )
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.reading_count,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM without_matches s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    reading_count,
    reading_read_at,
    reading_kwh,
    reading_quality,
    reading_voltage,
    UPPER(TRIM(quality)) AS quality_clean,
    COALESCE(reading_voltage, 0.0) AS reading_voltage_filled
FROM detail_rows
ORDER BY reading_read_at DESC NULLS FIRST
