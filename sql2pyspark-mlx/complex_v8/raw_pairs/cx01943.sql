WITH prepared AS (
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
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        listener_id,
        session_no,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_no
),
labeled AS (
    -- Add calculated columns
    SELECT
        listener_id,
        session_no,
        session_start,
        session_end,
        session_ms_played,
        duration_min,
        DATE_FORMAT(session_start, 'MMM yyyy') AS session_start_label,
        DATE_TRUNC('YEAR', session_end) AS session_end_year
    FROM sessions
    WHERE HOUR(session_start) BETWEEN 7 AND 13
)
SELECT
    listener_id,
    session_no,
    session_start,
    session_end,
    session_ms_played,
    session_start_label,
    session_end_year,
    CASE WHEN session_no >= 5 THEN 'large' WHEN session_no >= 2 THEN 'medium' ELSE 'small' END AS session_no_tier,
    DATEDIFF(DATE '2025-06-30', session_end) AS days_since_session_end
FROM labeled
WHERE session_ms_played <= 1440500
ORDER BY session_no DESC, listener_id NULLS LAST
