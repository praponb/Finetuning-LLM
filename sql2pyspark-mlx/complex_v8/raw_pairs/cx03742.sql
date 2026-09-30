WITH scoped AS (
    -- Keep valid rows and derive helper columns
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
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
agent_id_stats AS (
    -- Avg requester_id per agent_id
    SELECT agent_id, AVG(requester_id) AS avg_requester_id
    FROM joined
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM joined s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id > g.avg_requester_id
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.agent_name,
        s.agent_team,
        s.requester_id_minus_avg,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team_2,
        su.tier AS agent_tier_2
    FROM compared s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, csat, requester_id
    FROM combined_details_agent
) src
UNPIVOT (
    amount FOR metric_name IN (csat, requester_id)
) unp
