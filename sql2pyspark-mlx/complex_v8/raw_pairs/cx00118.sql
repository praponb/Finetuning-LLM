WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'album'
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
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
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
),
derived AS (
    -- Derive helper columns
    SELECT
        source,
        session_no,
        session_start,
        n_events,
        duration_min,
        LEAST(n_events, session_ms_played) AS least_n_events_session_ms_played,
        LENGTH(TRIM(source)) AS source_len
    FROM session_summary
    WHERE MONTH(session_end) IN (1, 2)
)
SELECT
    source,
    session_no,
    session_start,
    n_events,
    least_n_events_session_ms_played,
    session_start + INTERVAL 7 DAYS AS session_start_plus_7d,
    ABS(n_events - 3) AS n_events_distance
FROM derived
