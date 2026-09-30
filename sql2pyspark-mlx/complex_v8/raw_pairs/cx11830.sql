WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 12
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY read_at), 3) AS read_at_percent_rank
    FROM recent
),
grouped AS (
    -- Group-level totals
    SELECT
        site_id,
        TO_DATE(read_at) AS read_at_day,
        COUNT(*) AS row_count,
        MAX(kwh) AS max_kwh,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        ROUND(SUM(read_at_cume_dist) / NULLIF(SUM(kwh), 0), 3) AS read_at_cume_dist_to_kwh_ratio,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM with_windows
    WHERE read_at_cume_dist BETWEEN 0.3 AND 0.8
    GROUP BY site_id, TO_DATE(read_at)
    HAVING SUM(read_at_percent_rank) > 0.2
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM grouped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'EST'
              AND m.kwh < 116.8
        )
)
-- Final projection
SELECT
    site_id,
    read_at_day,
    row_count,
    distinct_qualitys,
    read_at_cume_dist_to_kwh_ratio,
    quality_list,
    LEAST(max_kwh, distinct_qualitys) AS least_max_kwh_distinct_qualitys,
    ABS(row_count - 9) AS row_count_distance
FROM qualified
ORDER BY row_count_distance, site_id, read_at_day
LIMIT 10
