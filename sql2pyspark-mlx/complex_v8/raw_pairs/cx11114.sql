WITH cleaned AS (
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
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-28 19:00:00'
),
agent_id_stats AS (
    -- Max csat per agent_id
    SELECT agent_id, MAX(csat) AS max_csat
    FROM cleaned
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM cleaned s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat = g.max_csat
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id AND su.team = 'Tech'
),
latest AS (
    -- Keep the latest row per agent_team
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
        max_csat,
        csat_minus_max,
        agent_tier,
        agent_team,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team ORDER BY closed_at DESC, id DESC) AS rn
        FROM enriched_agent
    ) ranked
    WHERE rn = 1
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(agent_team, 'ALL') AS agent_team,
    COALESCE(channel, 'ALL') AS channel,
    SUM(csat) FILTER (WHERE channel NOT IN ('email', 'chat')) AS csat_channel_not_in_email_chat
FROM latest
GROUP BY GROUPING SETS ((priority, agent_team, channel), (priority), (agent_team), ())
ORDER BY priority NULLS LAST, agent_team NULLS LAST, channel NULLS LAST
