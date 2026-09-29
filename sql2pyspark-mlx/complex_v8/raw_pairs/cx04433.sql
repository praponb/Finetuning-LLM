WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        CASE WHEN requester_id >= 23 THEN 'large' WHEN requester_id >= 9 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.csat,
        s.priority_len,
        s.priority_ref,
        s.requester_id_tier,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
requester_id_tier_stats AS (
    -- Avg requester_id per requester_id_tier
    SELECT requester_id_tier, AVG(requester_id) AS avg_requester_id
    FROM joined_agent
    GROUP BY requester_id_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM joined_agent s
    JOIN requester_id_tier_stats g ON g.requester_id_tier = s.requester_id_tier
    WHERE s.requester_id > g.avg_requester_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM outliers s
),
per_group AS (
    -- Group-level totals
    SELECT
        agent_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        SUM(priority_len) AS total_priority_len,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id
    FROM with_counts
    GROUP BY agent_id
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    agent_id,
    cnt,
    distinct_agent_tiers,
    total_priority_len,
    median_requester_id
FROM per_group
ORDER BY distinct_agent_tiers NULLS LAST, median_requester_id DESC
