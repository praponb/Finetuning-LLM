WITH scoped AS (
    -- Filter plays
    SELECT *
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM session_summary
    WHERE duration_min > (SELECT AVG(duration_min) FROM session_summary)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE duration_min > (SELECT AVG(duration_min) FROM without_matches)
)
SELECT
    source,
    session_no,
    session_start,
    session_end,
    n_events,
    duration_min,
    ROUND(session_no / NULLIF(duration_min, 0), 3) AS session_no_per_duration_min,
    ROUND(duration_min * 1.5, 2) AS duration_min_adj
FROM qualified
ORDER BY session_end DESC
