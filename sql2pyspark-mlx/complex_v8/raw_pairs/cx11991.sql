WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        CASE WHEN kwh >= 92.5 THEN 'large' WHEN kwh >= 37.0 THEN 'medium' ELSE 'small' END AS kwh_tier,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE kwh <= 110.5
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM recent_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        quality,
        quality_ref,
        kwh_tier,
        quality_or_unknown,
        reading_count,
        FIRST_VALUE(reading_count) IGNORE NULLS OVER (PARTITION BY quality_or_unknown, kwh_tier ORDER BY reading_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reading_count,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY MONTH(read_at) ORDER BY reading_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at) ORDER BY reading_count, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4
    FROM profiled
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY reading_count ORDER BY reading_count ASC) AS reading_count_dense_rank,
        MIN(kwh_avg_4) OVER (PARTITION BY first_known_reading_count, quality) AS first_known_reading_count_quality_min_kwh_avg_4,
        ROUND(AVG(kwh_avg_4) OVER w_first_known_reading_count_qu, 2) AS first_known_reading_count_quality_avg_kwh_avg_4,
        MAX(first_known_reading_count) OVER (PARTITION BY first_known_reading_count, quality) AS first_known_reading_count_quality_max_first_know,
        COUNT(*) OVER w_first_known_reading_count_qu AS rows_in_first_known_reading_count_quality
    FROM ranked
    WINDOW w_first_known_reading_count_qu AS (PARTITION BY first_known_reading_count, quality)
)
-- Subtotals with GROUPING SETS
SELECT
    reading_count,
    quality_or_unknown,
    COUNT(*) AS row_count,
    ROUND(SUM(reading_count_dense_rank) / NULLIF(SUM(first_known_reading_count_quality_avg_kwh_avg_4), 0), 3) AS reading_count_dense_rank_to_first_known_reading_
FROM windowed
GROUP BY GROUPING SETS ((reading_count, quality_or_unknown), (reading_count), (quality_or_unknown))
ORDER BY reading_count NULLS LAST, quality_or_unknown NULLS LAST
