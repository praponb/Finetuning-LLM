WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 9 AND 14
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(CASE WHEN m.quality IN ('EST', 'BAD') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count_2
    FROM valid s
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    reading_quality_count,
    GROUPING_ID() AS grouping_level,
    MIN(read_at) AS first_read_at,
    MIN(reading_quality_count) AS min_reading_quality_count
FROM with_stats
GROUP BY GROUPING SETS ((site_id, reading_quality_count), (site_id), ())
ORDER BY site_id NULLS LAST, reading_quality_count NULLS LAST
