WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE device = 'speaker'
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        track_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_id
),
per_group AS (
    -- Group-level totals
    SELECT
        events,
        session_id,
        COUNT(*) AS n_rows,
        MAX(session_id) AS max_session_id,
        MAX(session_start) AS last_session_start,
        ROUND(STDDEV(duration_min), 2) AS stddev_duration_min
    FROM per_session
    WHERE events <= 4
    GROUP BY events, session_id
    HAVING COUNT(*) >= 2
)
-- Subtotals with GROUPING SETS
SELECT
    session_id,
    n_rows,
    GROUPING(session_id) AS is_total_session_id,
    MIN(last_session_start) AS first_last_session_start
FROM per_group
GROUP BY GROUPING SETS ((session_id, n_rows), (session_id), ())
ORDER BY session_id NULLS LAST, n_rows NULLS LAST
