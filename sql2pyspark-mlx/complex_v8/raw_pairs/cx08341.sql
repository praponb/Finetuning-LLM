WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.prev_opened_at,
        s.new_session_flag,
        s.session_no,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM sessionized s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM with_parent_agent s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IN ('Onboarding', 'Tech')
              AND UPPER(TRIM(su.agent_name)) LIKE 'J%'
        )
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    session_no,
    agent_hired_on,
    agent_name
FROM qualified
ORDER BY agent_id DESC, opened_at DESC NULLS FIRST
