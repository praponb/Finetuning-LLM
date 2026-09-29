WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-26 18:00:00'
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        priority,
        session_id,
        session_start,
        session_end,
        events,
        session_csat,
        duration_min,
        CASE WHEN priority IN ('P1', 'P2') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM session_summary
)
SELECT
    priority,
    session_id,
    session_start,
    session_end,
    events,
    session_csat,
    duration_min,
    priority_group
FROM annotated
WHERE priority = 'P4'
ORDER BY priority_group NULLS LAST, duration_min DESC
