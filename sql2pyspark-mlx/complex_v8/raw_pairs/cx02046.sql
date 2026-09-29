WITH prepared AS (
    -- Filter plays
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
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        source,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, session_no
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        source,
        session_no,
        events,
        NTILE(10) OVER (PARTITION BY source, session_no ORDER BY session_end, source, session_no) AS session_end_decile,
        MIN(events) OVER (PARTITION BY session_no) AS session_no_min_events,
        session_no - LAG(session_no) OVER w_source_session_no_by_session AS session_no_change,
        FIRST_VALUE(duration_min) OVER (PARTITION BY source, session_no ORDER BY session_end, source, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_min,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY session_no), 0), 4) AS session_no_share_of_session_no
    FROM session_summary
    WINDOW w_source_session_no_by_session AS (PARTITION BY source, session_no ORDER BY session_end, source, session_no)
)
-- Final projection
SELECT
    source,
    events,
    session_end_decile,
    session_no_min_events,
    session_no_change,
    first_duration_min,
    session_no_share_of_session_no,
    CASE WHEN session_no >= 4 THEN 'A' WHEN session_no >= 3 THEN 'B' ELSE 'C' END AS session_no_tier,
    SUBSTRING(TRIM(source), 1, 3) AS source_prefix
FROM with_windows
ORDER BY events
