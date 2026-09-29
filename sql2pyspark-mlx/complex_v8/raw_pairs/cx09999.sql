WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 9 AND 11
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality IS NOT NULL THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM base_readings s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(reading_quality_count / NULLIF(SUM(reading_quality_count) OVER (PARTITION BY site_id), 0), 4) AS reading_quality_count_share_of_site_id,
        ROUND((UNIX_TIMESTAMP(last_reading_read_at) - UNIX_TIMESTAMP(LAG(last_reading_read_at) OVER (PARTITION BY quality, MONTH(last_reading_read_at) ORDER BY read_at, id))) / 60, 1) AS minutes_since_prev_last_reading_read_at,
        COUNT(*) OVER w_site_id AS rows_in_site_id
    FROM with_counts
    WINDOW w_site_id AS (PARTITION BY site_id)
),
site_id_stats AS (
    -- Avg kwh per site_id
    SELECT site_id, AVG(kwh) AS avg_kwh
    FROM metrics
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM metrics s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh > g.avg_kwh
)
-- Subtotals with GROUPING SETS
SELECT
    reading_quality_count,
    quality,
    site_id,
    SUM(CASE WHEN quality NOT IN ('OK', 'EST') THEN minutes_since_prev_last_reading_read_at ELSE 0 END) AS minutes_since_prev_last_reading_read_at_quality_,
    ROUND(SUM(avg_kwh) / NULLIF(SUM(reading_quality_count), 0), 3) AS avg_kwh_to_reading_quality_count_ratio
FROM vs_group
GROUP BY GROUPING SETS ((reading_quality_count, quality, site_id), (reading_quality_count))
ORDER BY reading_quality_count NULLS LAST, quality NULLS LAST, site_id NULLS LAST
