WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 12:00:00'
),
site_id_stats AS (
    -- Median kwh per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM valid
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM valid s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh >= g.median_kwh
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM outliers s
    JOIN sites si ON si.id = s.site_id
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.read_at_day,
        s.kwh_minus_median,
        s.site_installed_kw,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id AND si.installed_kw <= 373.3
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        read_at_day,
        kwh_minus_median,
        site_installed_kw,
        site_name,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM joined
    WHERE quality IN ('OK', 'EST')
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM derived s
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality,
    read_at_day,
    kwh_minus_median,
    site_name,
    quality_or_unknown,
    total_reading_voltage,
    reading_count,
    reading_count + site_installed_kw AS reading_count_plus_site_installed_kw,
    ABS(reading_count - 2) AS reading_count_distance
FROM with_stats
ORDER BY read_at_day DESC NULLS FIRST, kwh, id
LIMIT 5
