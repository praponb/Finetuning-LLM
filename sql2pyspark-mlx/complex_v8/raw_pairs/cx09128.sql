WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 10 AND 14
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.days_since_opened_at,
        s.priority_clean,
        su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN actor IN ('system', 'agent') THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM combined_details_agent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    agent_team,
    GROUPING(agent_id) AS is_total_agent_id,
    COUNT(DISTINCT id) AS distinct_ids
FROM profiled
GROUP BY GROUPING SETS ((agent_id, agent_team), (agent_id), ())
