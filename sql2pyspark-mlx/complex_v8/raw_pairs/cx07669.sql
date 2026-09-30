WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE site_id <= 5
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT ROUND(AVG(m.kwh), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_kwh,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh
    FROM prepared s
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        quality,
        min_reading_kwh,
        MAX(min_reading_kwh) OVER (PARTITION BY site_id ORDER BY last_reading_read_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS min_reading_kwh_max_3,
        SUM(kwh) OVER (PARTITION BY quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY quality), 2) AS quality_avg_kwh
    FROM with_stats
),
scored AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM ranked
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        quality,
        min_reading_kwh,
        min_reading_kwh_max_3,
        running_kwh,
        quality_avg_kwh,
        quality_prefix,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix_2
    FROM scored
    WHERE running_kwh < 720.0
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    min_reading_kwh,
    min_reading_kwh_max_3,
    running_kwh,
    quality_avg_kwh,
    quality_prefix,
    quality_prefix_2
FROM enriched
WHERE TO_DATE(read_at) < DATE '2025-02-18'
ORDER BY quality_prefix, quality_avg_kwh DESC NULLS FIRST
