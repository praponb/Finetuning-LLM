WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_no
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE session_csat > (SELECT AVG(session_csat) FROM per_session)
)
-- Final projection
SELECT
    channel,
    session_no,
    session_end,
    events,
    session_csat,
    duration_min
FROM screened
ORDER BY session_no, events DESC NULLS FIRST
