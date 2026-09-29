WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        ABS(ms_played - 107912) AS ms_played_distance,
        CASE WHEN ms_played >= 239955 THEN 'HIGH' WHEN ms_played >= 98052 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d
    FROM plays
    WHERE source = 'search'
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN source IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_source_is_not_null
    FROM filtered_plays
    GROUP BY DATE_FORMAT(played_at, 'yyyy-MM')
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN n_rows >= 10 THEN 'A' WHEN n_rows >= 5 THEN 'B' ELSE 'C' END AS n_rows_tier
    FROM summary
)
-- Final projection
SELECT played_at_ym, n_rows, pct_source_is_not_null
FROM annotated
ORDER BY pct_source_is_not_null
