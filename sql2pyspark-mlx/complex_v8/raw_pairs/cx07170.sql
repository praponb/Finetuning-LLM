WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM tickets
    WHERE (csat > 3 OR agent_id <= 2)
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM filtered_tickets s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
      AND s.csat BETWEEN 1 AND 5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.agent_name
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_priority_label,
        s.minutes_opened_at_to_closed_at,
        s.csat_tier,
        s.agent_tier,
        s.agent_team,
        s.agent_name,
        su.team AS agent_team_2,
        su.tier AS agent_tier_2,
        su.agent_name AS agent_agent_name
    FROM with_parent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_tier,
    agent_tier_2,
    csat_tier,
    GROUPING_ID() AS grouping_level,
    SUM(minutes_opened_at_to_closed_at) AS total_minutes_opened_at_to_closed_at
FROM with_details
GROUP BY ROLLUP(agent_tier, agent_tier_2, csat_tier)
ORDER BY agent_tier NULLS LAST, agent_tier_2 NULLS LAST, csat_tier NULLS LAST
