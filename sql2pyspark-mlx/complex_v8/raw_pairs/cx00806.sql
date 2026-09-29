WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
),
site_id_stats AS (
    -- Median kwh per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM filtered
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM filtered s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh >= g.median_kwh
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_slug,
        quality_prefix,
        median_kwh,
        kwh_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM above_average
    ) ranked
    WHERE rn = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        MIN(read_at) AS first_reading_read_at,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM deduped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    site_id,
    quality,
    reading_count,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    ROUND(100 * COUNT(CASE WHEN quality IN ('EST', 'OK') THEN 1 END) / COUNT(*), 1) AS pct_quality_in_est_ok
FROM with_stats
GROUP BY CUBE(site_id, quality, reading_count)
ORDER BY site_id NULLS LAST, quality NULLS LAST, reading_count NULLS LAST
