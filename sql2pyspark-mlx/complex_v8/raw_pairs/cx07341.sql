WITH recent AS (
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
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 10 AND 14
      AND requester_id >= 7
),
priority_stats AS (
    -- Max csat per priority
    SELECT priority, MAX(csat) AS max_csat
    FROM recent
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM recent s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.csat = g.max_csat
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.max_csat,
        s.csat_minus_max,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM compared s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        csat_minus_max,
        agent_name,
        agent_hired_on,
        ROUND(AVG(max_csat) OVER (PARTITION BY channel), 2) AS channel_avg_max_csat,
        LEAD(closed_at) OVER (PARTITION BY csat ORDER BY requester_id, id) AS next_closed_at,
        ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY agent_hired_on DESC, id) AS rn
    FROM enriched_agent
)
-- Subtotals with ROLLUP
SELECT
    channel,
    csat,
    agent_id,
    ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
    MIN(csat) AS min_csat,
    COUNT(*) AS cnt
FROM sequenced
GROUP BY ROLLUP(channel, csat, agent_id)
