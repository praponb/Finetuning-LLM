WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE requester_id > 7
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-25 10:00:00'
              AND t.actor = 'customer'
        )
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.tags_text,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM without_matches s
    JOIN support_agents su ON su.id = s.agent_id AND YEAR(su.hired_on) = 2025
)
SELECT
    COALESCE(agent_team, 'ALL') AS agent_team,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
    ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list,
    ROUND(100 * COUNT(CASE WHEN channel = 'web' THEN 1 END) / COUNT(*), 1) AS pct_channel_web
FROM with_parent_agent
GROUP BY GROUPING SETS ((agent_team, agent_tier), (agent_team), (agent_tier))
