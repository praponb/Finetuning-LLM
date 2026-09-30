WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE (read_at < TIMESTAMP '2025-02-24 10:00:00' OR voltage <= 233.1)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        read_at_quarter,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM recent
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM enriched s
)
-- Subtotals with GROUPING SETS
SELECT
    reading_count,
    quality_group,
    reading_count_2,
    MAX(reading_count_2) AS max_reading_count_2
FROM with_stats
GROUP BY GROUPING SETS ((reading_count, quality_group, reading_count_2), (reading_count), (quality_group))
ORDER BY reading_count NULLS LAST, quality_group NULLS LAST, reading_count_2 NULLS LAST
