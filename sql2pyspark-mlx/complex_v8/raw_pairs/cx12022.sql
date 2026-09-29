WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE (voltage <= 237.5 OR kwh > 28.4)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_date,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 284.0
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-12-31', read_at_date) AS days_since_read_at_date
    FROM with_details
),
quality_stats AS (
    -- Avg kwh per quality
    SELECT quality, AVG(kwh) AS avg_kwh
    FROM enriched
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM enriched s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh > g.avg_kwh
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh
    FROM outliers s
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(total_reading_kwh / 100) * 100 AS total_reading_kwh_bucket,
        SUBSTRING(TRIM(site_zone), 1, 4) AS site_zone_prefix
    FROM summarized_rows
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    site_installed_kw,
    site_zone,
    site_commissioned_on,
    days_since_read_at_date,
    kwh_minus_avg,
    total_reading_kwh,
    total_reading_kwh_bucket
FROM annotated
ORDER BY kwh DESC NULLS FIRST, total_reading_kwh_bucket DESC NULLS FIRST
