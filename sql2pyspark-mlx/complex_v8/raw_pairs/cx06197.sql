WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE device IS NOT NULL
),
source_stats AS (
    -- Median ms_played per source
    SELECT source, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM recent_plays
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM recent_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played >= g.median_ms_played
)
SELECT
    COALESCE(source_group, 'ALL') AS source_group,
    COALESCE(source, 'ALL') AS source,
    GROUPING(source_group) AS is_total_source_group,
    SUM(ms_played_minus_median) AS total_ms_played_minus_median,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM above_average
GROUP BY ROLLUP(source_group, source)
ORDER BY source_group NULLS LAST, source NULLS LAST
