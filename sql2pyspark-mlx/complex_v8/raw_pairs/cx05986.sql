WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE agent_id <= 2
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.minutes_closed_at_to_opened_at,
        s.is_new_session,
        s.burst_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    priority,
    channel,
    csat,
    minutes_closed_at_to_opened_at,
    is_new_session,
    burst_id,
    agent_hired_on,
    agent_tier,
    agent_name,
    DATEDIFF(DATE '2026-01-01', agent_hired_on) AS days_since_agent_hired_on
FROM with_parent_agent
ORDER BY csat NULLS LAST, priority DESC
