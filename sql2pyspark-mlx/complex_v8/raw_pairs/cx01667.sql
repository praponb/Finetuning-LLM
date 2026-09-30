WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        MAX(csat) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_max_2,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel), 0), 4) AS csat_share_of_channel,
        MAX(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_max_3_prior,
        ROUND(csat - AVG(csat) OVER w_channel, 2) AS csat_vs_channel_avg,
        MIN(csat) OVER (PARTITION BY channel, csat) AS channel_csat_min_csat,
        ROUND(AVG(csat) OVER (PARTITION BY TO_DATE(opened_at), csat ORDER BY closed_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        MIN(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel, csat), 0), 4) AS csat_share_of_channel_csat
    FROM recent_tickets
    WINDOW w_channel AS (PARTITION BY channel)
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    csat,
    tags,
    csat_max_2,
    csat_share_of_channel,
    requester_id_max_3_prior,
    csat_avg_7,
    min_requester_id_so_far,
    distinct_agent_id_in_channel,
    csat_share_of_channel_csat
FROM ranked
ORDER BY requester_id_max_3_prior NULLS LAST, distinct_agent_id_in_channel
