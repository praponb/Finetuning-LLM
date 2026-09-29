WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        channel,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_no
),
derived AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(session_end, 'yyyy-MM-dd') AS session_end_label
    FROM session_summary
)
SELECT
    channel,
    session_start,
    event_count,
    session_csat,
    session_end_label
FROM derived
WHERE channel NOT IN ('web', 'chat')
ORDER BY session_end_label, session_csat
