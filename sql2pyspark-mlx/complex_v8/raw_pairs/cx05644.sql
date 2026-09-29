WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
stats AS (
    -- Group-level totals
    SELECT
        listener_id,
        DATE_FORMAT(prev_played_at, 'yyyy-MM') AS prev_played_at_ym,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sessionized
    GROUP BY listener_id, DATE_FORMAT(prev_played_at, 'yyyy-MM')
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    distinct_ids,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(distinct_ids), 2) AS stddev_distinct_ids,
    MAX(distinct_ids) AS max_distinct_ids,
    PERCENTILE_APPROX(distinct_ids, 0.5) AS median_distinct_ids
FROM stats
GROUP BY GROUPING SETS ((listener_id, distinct_ids), (listener_id), ())
ORDER BY listener_id NULLS LAST, distinct_ids NULLS LAST
