WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-06 15:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        ROW_NUMBER() OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC NULLS FIRST, id) AS rn,
        SIZE(COLLECT_SET(agent_id) OVER w_channel) AS distinct_agent_id_in_channel,
        ROUND(AVG(csat) OVER (ORDER BY csat, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority, agent_id), 2) AS requester_id_vs_priority_agent_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id DESC), 3) AS requester_id_cume_dist,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY channel)) AS distinct_priority_in_channel,
        MAX(csat) OVER (PARTITION BY channel) AS channel_max_csat,
        ROUND(AVG(csat) OVER (ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        LEAD(opened_at, 3) OVER (ORDER BY opened_at ASC NULLS LAST, id) AS next3_opened_at,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority, agent_id), 2) AS csat_vs_priority_agent_id_avg
    FROM prepared
    WINDOW w_channel AS (PARTITION BY channel)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(csat_avg_3 * 0.9, 2) AS csat_avg_3_adj,
        ROUND(csat_avg_2 * 1.07, 2) AS csat_avg_2_adj
    FROM metrics
    WHERE next3_opened_at < TIMESTAMP '2025-02-18 13:00:00'
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    rn,
    csat_avg_2,
    requester_id_vs_priority_agent_id_avg,
    requester_id_cume_dist,
    distinct_priority_in_channel,
    channel_max_csat,
    csat_avg_3,
    next3_opened_at,
    csat_vs_priority_agent_id_avg,
    csat_avg_2_adj
FROM scored
ORDER BY csat_avg_2
