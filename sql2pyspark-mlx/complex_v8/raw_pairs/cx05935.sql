WITH scoped AS (
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
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 10 AND 13
      AND csat BETWEEN 1 AND 4
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.priority_prefix,
        s.channel_clean,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 8
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        agent_tier,
        agent_hired_on,
        ROUND(AVG(csat) OVER (PARTITION BY csat, priority), 2) AS csat_priority_avg_csat,
        COUNT(*) OVER (PARTITION BY csat, priority) AS rows_in_csat_priority,
        MIN(csat) OVER (PARTITION BY csat, priority) AS csat_priority_min_csat,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY csat, priority)) AS distinct_agent_tier_in_csat_priority,
        ROUND(CUME_DIST() OVER (ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_cume_dist,
        COUNT(*) OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (ORDER BY agent_hired_on DESC, id)) AS days_since_prev_agent_hired_on,
        csat - LAG(csat) OVER (ORDER BY agent_hired_on DESC, id) AS csat_change,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat, priority)) AS distinct_channel_in_csat_priority,
        RANK() OVER (ORDER BY csat, id) AS csat_rank,
        DENSE_RANK() OVER (ORDER BY csat) AS csat_dense_rank
    FROM with_details
)
-- Final projection
SELECT
    id,
    closed_at,
    channel,
    tags,
    agent_tier,
    agent_hired_on,
    csat_priority_avg_csat,
    rows_in_csat_priority,
    csat_priority_min_csat,
    agent_hired_on_cume_dist,
    running_count,
    days_since_prev_agent_hired_on,
    csat_change,
    distinct_channel_in_csat_priority,
    csat_rank,
    ROUND(running_count / NULLIF(csat_priority_avg_csat, 0), 3) AS running_count_per_csat_priority_avg_csat
FROM sequenced
