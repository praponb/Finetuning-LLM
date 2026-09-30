WITH valid AS (
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
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        source,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(days_since_played_at) AS session_days_since_played_at,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
),
totals AS (
    -- Group-level totals
    SELECT
        source,
        session_no,
        SUM(session_days_since_played_at) AS total_session_days_since_played_at,
        MIN(session_days_since_played_at) AS min_session_days_since_played_at,
        ROUND(AVG(duration_min), 2) AS avg_duration_min,
        COUNT(*) AS n_rows
    FROM per_session
    GROUP BY 1, 2
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE avg_duration_min > (SELECT AVG(avg_duration_min) FROM totals)
)
SELECT *
FROM (
    SELECT n_rows, session_no, source, min_session_days_since_played_at
    FROM without_matches
) src
PIVOT (
    SUM(min_session_days_since_played_at)
    FOR source IN ('search', 'album', 'playlist', 'radio')
)
ORDER BY n_rows, session_no
