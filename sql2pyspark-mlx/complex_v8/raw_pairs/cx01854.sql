WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE voltage < 235.8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY quality ORDER BY kwh) AS kwh_rank,
        MAX(kwh) OVER w_quality AS quality_max_kwh,
        LAG(read_at) OVER (PARTITION BY quality ORDER BY kwh, id) AS prev_read_at,
        kwh - LAG(kwh) OVER (PARTITION BY quality ORDER BY kwh, id) AS kwh_change,
        FIRST_VALUE(kwh) OVER (PARTITION BY quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh
    FROM filtered_readings
    WINDOW w_quality AS (PARTITION BY quality)
),
quality_stats AS (
    -- Max first_kwh per quality
    SELECT quality, MAX(first_kwh) AS max_first_kwh
    FROM with_trends
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_first_kwh,
        ROUND(s.first_kwh - g.max_first_kwh, 2) AS first_kwh_minus_max
    FROM with_trends s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.first_kwh = g.max_first_kwh
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM outliers s
)
SELECT
    site_id,
    kwh_rank,
    quality,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    ROUND(AVG(first_kwh), 2) AS avg_first_kwh,
    ROUND(STDDEV(kwh_change), 2) AS stddev_kwh_change
FROM with_stats
GROUP BY GROUPING SETS ((site_id, kwh_rank, quality), (site_id), (kwh_rank), ())
ORDER BY site_id NULLS LAST, kwh_rank NULLS LAST, quality NULLS LAST
