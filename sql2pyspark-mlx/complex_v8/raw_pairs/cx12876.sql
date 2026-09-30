WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix_2
    FROM plays
    WHERE ms_played < 287769
),
annotated AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM valid_plays
    WHERE listener_id <= 11
),
rollup_base AS (
    -- Group-level totals
    SELECT
        listener_id,
        YEAR(played_at) AS played_at_year,
        CASE WHEN ms_played >= 223828 THEN 'A' WHEN ms_played >= 129336 THEN 'B' ELSE 'C' END AS ms_played_tier,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        SUM(CASE WHEN is_skipped = FALSE THEN ms_played ELSE 0 END) AS ms_played_is_skipped_false,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_false
    FROM annotated
    WHERE device_prefix IS NOT NULL
    GROUP BY listener_id, YEAR(played_at), CASE WHEN ms_played >= 223828 THEN 'A' WHEN ms_played >= 129336 THEN 'B' ELSE 'C' END
    HAVING COUNT(*) >= 2
)
SELECT *
FROM (
    SELECT listener_id, ms_played_tier, stddev_ms_played
    FROM rollup_base
) src
PIVOT (
    MIN(stddev_ms_played)
    FOR ms_played_tier IN ('C', 'B')
)
ORDER BY listener_id
