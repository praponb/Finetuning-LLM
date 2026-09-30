WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 14:00:00'
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        session_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        SUM(session_id) AS total_session_id,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM per_session
    WHERE priority NOT IN ('P2', 'P1')
    GROUP BY DATE_TRUNC('MONTH', session_start)
)
SELECT
    session_start_month,
    total_session_id,
    priority_list,
    SIZE(priority_list) AS priority_list_count
FROM rollup_base
ORDER BY priority_list_count DESC NULLS FIRST, total_session_id DESC
