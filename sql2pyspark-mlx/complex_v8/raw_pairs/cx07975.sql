WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE tariff = 'FLAT'
      AND UPPER(TRIM(site_name)) LIKE 'D%'
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count,
        (SELECT COUNT(CASE WHEN m.quality IN ('EST', 'BAD') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS max_reading_voltage
    FROM scoped_sites s
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.installed_kw,
        s.commissioned_on,
        s.reading_count,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM with_stats s
    LEFT JOIN meter_readings m ON m.site_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(zone, '#', CAST(reading_id AS STRING)) AS zone_ref,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM detail_rows_readings
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(reading_kwh, 2) OVER (ORDER BY commissioned_on DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh,
        MIN(reading_kwh) OVER (PARTITION BY MONTH(reading_read_at), reading_count) AS reading_read_at_month_reading_count_min_reading_,
        NTH_VALUE(commissioned_on, 2) OVER (ORDER BY commissioned_on DESC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on,
        NTILE(3) OVER (ORDER BY commissioned_on DESC, id, reading_id) AS commissioned_on_tercile
    FROM derived
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS min_reading_voltage,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS total_reading_voltage,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.id) AS last_reading_read_at
    FROM metrics s
)
SELECT
    id,
    zone,
    reading_count,
    reading_id,
    reading_read_at,
    zone_slug,
    second_reading_kwh,
    reading_read_at_month_reading_count_min_reading_,
    second_commissioned_on,
    min_reading_voltage,
    total_reading_voltage,
    last_reading_read_at
FROM summarized_rows
ORDER BY second_commissioned_on
