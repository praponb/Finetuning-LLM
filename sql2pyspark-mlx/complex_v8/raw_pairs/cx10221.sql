WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE device NOT IN ('tv', 'phone')
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM session_summary s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND HOUR(p.played_at) BETWEEN 7 AND 11
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        listener_id,
        session_no,
        session_start,
        session_end,
        n_events,
        session_ms_played,
        ROUND(duration_min * 1.5, 2) AS duration_min_adj,
        ROUND(n_events / NULLIF(session_no, 0), 3) AS n_events_per_session_no,
        DATE_TRUNC('QUARTER', session_start) AS session_start_quarter
    FROM screened
)
SELECT
    listener_id,
    session_no,
    session_start,
    session_end,
    session_ms_played,
    duration_min_adj,
    session_start_quarter,
    TO_DATE(session_start_quarter) AS session_start_quarter_date,
    ROUND(n_events_per_session_no / NULLIF(n_events, 0), 3) AS n_events_per_session_no_per_n_events
FROM labeled
ORDER BY listener_id DESC NULLS FIRST, session_ms_played DESC, session_no
LIMIT 5
