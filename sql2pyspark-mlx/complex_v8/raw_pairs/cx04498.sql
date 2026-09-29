WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel = 'chat'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2,
        COUNT(CASE WHEN actor = 'customer' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM recent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.tags,
        s.first_event_at,
        s.first_event_at_2,
        s.event_actor_count,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name
    FROM summarized_rows s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.first_event_at,
        s.first_event_at_2,
        s.event_actor_count,
        s.agent_team,
        s.agent_tier,
        s.agent_name,
        t.tag
    FROM with_details s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
)
-- Subtotals with CUBE
SELECT
    agent_team,
    tag,
    agent_tier,
    ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list
FROM flattened
GROUP BY CUBE(agent_team, tag, agent_tier)
ORDER BY agent_team NULLS LAST, tag NULLS LAST, agent_tier NULLS LAST
