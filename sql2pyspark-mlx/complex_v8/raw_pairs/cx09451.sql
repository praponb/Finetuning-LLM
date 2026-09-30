WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE (read_at < TIMESTAMP '2025-02-24 10:00:00' OR voltage >= 216.8)
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh
    FROM recent_readings s
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_minus_voltage,
        s.voltage_per_kwh,
        s.first_reading_read_at,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM with_counts s
    JOIN sites si ON si.id = s.site_id
    WHERE YEAR(si.commissioned_on) = 2024
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw_2, si.site_name
    FROM with_details s
    JOIN sites si ON si.id = s.site_id
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.voltage_per_kwh,
        s.site_name,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM with_parent_site s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    COALESCE(reading_quality, 'ALL') AS reading_quality,
    site_id,
    SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'D%' THEN reading_voltage ELSE 0 END) AS reading_voltage_upper_trim_site_name_like_d,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM exploded_detail
GROUP BY GROUPING SETS ((reading_quality, site_id), (reading_quality), ())
