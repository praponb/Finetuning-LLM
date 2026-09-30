WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags
    FROM tickets
    WHERE MONTH(closed_at) IN (2, 3)
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        agent_id,
        session_id,
        session_start,
        session_end,
        n_events,
        COUNT(*) OVER (PARTITION BY agent_id, session_id) AS rows_in_agent_id_session_id,
        ROUND(AVG(duration_min) OVER (ORDER BY duration_min, agent_id, session_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_min_avg_2
    FROM session_summary
)
-- Final projection
SELECT
    agent_id,
    session_id,
    session_start,
    session_end,
    rows_in_agent_id_session_id,
    DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start,
    CASE WHEN session_id >= 5 THEN 'large' WHEN session_id >= 3 THEN 'medium' ELSE 'small' END AS session_id_tier
FROM metrics
ORDER BY days_since_session_start NULLS LAST, agent_id DESC
