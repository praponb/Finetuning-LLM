WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 2 HOURS AS read_at_plus_2h,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE (site_id >= 9 OR kwh < 106.5)
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_len,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM recent_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.quality_len,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality,
        s.reading_voltage,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        m.read_at AS reading_read_at_2
    FROM detail_rows s
    LEFT JOIN sites si ON si.id = s.site_id
    JOIN meter_readings m ON m.id = s.reading_id
),
joined_reading AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality_len,
        s.reading_id,
        s.reading_read_at,
        s.reading_quality,
        s.reading_voltage,
        s.site_tariff,
        s.site_commissioned_on,
        s.reading_read_at_2,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality_2,
        m.read_at AS reading_read_at_3
    FROM enriched s
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE m.quality IS NOT NULL
),
per_group AS (
    -- Summarise per group
    SELECT
        quality_len,
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        site_id,
        MAX(reading_read_at) AS last_reading_read_at,
        PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM joined_reading
    GROUP BY quality_len, DATE_TRUNC('MONTH', site_commissioned_on), site_id
    HAVING AVG(reading_voltage) > 218.2
)
-- Final projection
SELECT
    quality_len,
    site_commissioned_on_month,
    site_id,
    last_reading_read_at,
    median_voltage
FROM per_group
ORDER BY quality_len DESC, site_commissioned_on_month, site_id
LIMIT 10
