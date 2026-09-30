WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.channel_len,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier <> 'T3'
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.channel_len,
        s.agent_team,
        s.agent_tier,
        su.hired_on AS agent_hired_on_2
    FROM joined_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
channel_stats AS (
    -- Avg csat per channel
    SELECT channel, AVG(csat) AS avg_csat
    FROM with_parent_agent
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM with_parent_agent s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat > g.avg_csat
),
with_parent_agent_2 AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.channel_len,
        s.agent_team,
        s.agent_hired_on_2,
        s.csat_minus_avg,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on < DATE '2025-02-27'
),
summary AS (
    -- Group-level totals
    SELECT
        channel,
        CASE WHEN csat_minus_avg >= 2.5 THEN 'HIGH' WHEN csat_minus_avg >= -2.5 THEN 'MEDIUM' ELSE 'LOW' END AS csat_minus_avg_tier,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(*) FILTER (WHERE agent_team IN ('VIP', 'Onboarding', 'Billing')) AS n_agent_team_in_vip_onboarding,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams
    FROM with_parent_agent_2
    GROUP BY channel, CASE WHEN csat_minus_avg >= 2.5 THEN 'HIGH' WHEN csat_minus_avg >= -2.5 THEN 'MEDIUM' ELSE 'LOW' END
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(csat_minus_avg_tier), 1, 4) AS csat_minus_avg_tier_prefix,
        LENGTH(TRIM(channel)) AS channel_len,
        CONCAT(csat_minus_avg_tier, '#', CAST(row_count AS STRING)) AS csat_minus_avg_tier_ref
    FROM summary
)
-- Subtotals with CUBE
SELECT
    row_count,
    channel_len,
    channel,
    GROUPING(row_count) AS is_total_row_count,
    ROUND(SUM(channel_len) / NULLIF(SUM(distinct_agent_teams), 0), 3) AS channel_len_to_distinct_agent_teams_ratio,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
FROM tagged
GROUP BY CUBE(row_count, channel_len, channel)
ORDER BY row_count NULLS LAST, channel_len NULLS LAST, channel NULLS LAST
