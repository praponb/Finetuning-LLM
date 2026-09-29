WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        ROUND(AVG(requester_id), 2) AS avg_ticket_requester_id,
        MAX(csat) AS max_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.avg_ticket_requester_id,
        t.max_ticket_csat
    FROM per_session s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    session_start,
    events,
    duration_min,
    avg_ticket_requester_id,
    max_ticket_csat
FROM filtered
ORDER BY duration_min DESC
