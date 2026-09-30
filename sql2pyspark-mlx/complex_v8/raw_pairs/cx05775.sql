WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.priority_or_unknown,
        s.prev_opened_at,
        s.new_session_flag,
        s.session_no,
        su.tier AS agent_tier
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(session_no / NULLIF(requester_id, 0), 3) AS session_no_per_requester_id,
        DATE_TRUNC('YEAR', prev_opened_at) AS prev_opened_at_year,
        DATE_TRUNC('YEAR', prev_opened_at) AS prev_opened_at_year_2
    FROM joined_agent
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    prev_opened_at,
    new_session_flag,
    session_no,
    agent_tier,
    session_no_per_requester_id,
    prev_opened_at_year,
    prev_opened_at_year_2
FROM tagged
ORDER BY requester_id DESC
