WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat
    FROM tickets
    WHERE channel IS NOT NULL
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at < TIMESTAMP '2025-03-08 11:00:00'
              AND SIZE(t.tags) >= 1
        )
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.agent_id,
        s.burst_id,
        s.session_end,
        s.event_count,
        s.session_csat,
        s.duration_min,
        su.agent_name
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id AND su.team = 'VIP'
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    session_end,
    event_count,
    session_csat,
    duration_min,
    COALESCE(session_csat, 0) AS session_csat_filled,
    TO_DATE(session_end) AS session_end_date
FROM joined
ORDER BY session_end DESC, burst_id NULLS LAST, agent_id
LIMIT 10
