WITH base_tickets AS (
    -- Filter tickets
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
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE priority = 'P4'
      AND closed_at >= TIMESTAMP '2025-01-26 18:00:00'
),
channel_stats AS (
    -- Avg requester_id per channel
    SELECT channel, AVG(requester_id) AS avg_requester_id
    FROM base_tickets
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM base_tickets s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.requester_id > g.avg_requester_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.avg_requester_id,
        s.requester_id_minus_avg,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T3', 'T1')
),
with_windows AS (
    -- Previous / next values per key
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
        requester_id_minus_avg,
        agent_team,
        agent_hired_on,
        agent_tier,
        LEAD(avg_requester_id) OVER (PARTITION BY YEAR(closed_at) ORDER BY opened_at, id) AS next_avg_requester_id,
        ROW_NUMBER() OVER w_closed_at_year_by_opened_at AS rn,
        ROUND(requester_id_minus_avg / NULLIF(SUM(requester_id_minus_avg) OVER w_agent_team_channel, 0), 4) AS requester_id_minus_avg_share_of_agent_team_chann,
        MAX(avg_requester_id) OVER w_agent_team_channel AS agent_team_channel_max_avg_requester_id
    FROM with_details
    WINDOW w_closed_at_year_by_opened_at AS (PARTITION BY YEAR(closed_at) ORDER BY opened_at, id), w_agent_team_channel AS (PARTITION BY agent_team, channel)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    requester_id_minus_avg,
    agent_hired_on,
    agent_tier,
    next_avg_requester_id,
    requester_id_minus_avg_share_of_agent_team_chann,
    agent_team_channel_max_avg_requester_id,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
    MONTH(closed_at) AS closed_at_month
FROM with_windows
ORDER BY agent_id NULLS LAST
