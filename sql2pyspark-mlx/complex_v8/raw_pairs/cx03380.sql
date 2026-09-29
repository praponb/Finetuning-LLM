WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
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
priority_group_stats AS (
    -- Avg requester_id per priority_group
    SELECT priority_group, AVG(requester_id) AS avg_requester_id
    FROM sessionized
    GROUP BY priority_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM sessionized s
    JOIN priority_group_stats g ON g.priority_group = s.priority_group
    WHERE s.requester_id > g.avg_requester_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    csat,
    starts_session,
    burst_id,
    avg_requester_id,
    requester_id_minus_avg,
    DATEDIFF(DATE '2025-12-31', prev_opened_at) AS days_since_prev_opened_at
FROM compared
ORDER BY requester_id DESC, id
LIMIT 20
