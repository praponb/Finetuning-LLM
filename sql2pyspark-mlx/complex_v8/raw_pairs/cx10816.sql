WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM valid_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY session_no ORDER BY requester_id DESC, id) AS requester_id_quintile,
        SUM(is_new_session) OVER (PARTITION BY session_no ORDER BY requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS is_new_session_sum_2
    FROM sessionized
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel_slug,
        s.prev_opened_at,
        s.is_new_session,
        s.session_no,
        s.requester_id_quintile,
        s.is_new_session_sum_2,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM with_trends s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    channel_slug,
    is_new_session,
    requester_id_quintile,
    is_new_session_sum_2,
    agent_tier,
    agent_team
FROM with_details_agent
ORDER BY agent_team DESC NULLS FIRST, id DESC NULLS FIRST
