WITH recent AS (
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
        CASE WHEN requester_id >= 21 THEN 'A' WHEN requester_id >= 8 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
    WHERE agent_id >= 7
      AND opened_at < TIMESTAMP '2025-03-02 08:00:00'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        requester_id_tier,
        csat - LAG(csat) OVER (ORDER BY closed_at DESC, id) AS csat_change,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY requester_id_tier, agent_id), 0), 4) AS csat_share_of_requester_id_tier_agent_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY requester_id_tier, agent_id)) AS distinct_channel_in_requester_id_tier_agent_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY csat DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(AVG(csat) OVER w_agent_id_csat, 2) AS agent_id_csat_avg_csat,
        ROW_NUMBER() OVER (ORDER BY csat DESC, id) AS csat_seq,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, csat), 0), 4) AS requester_id_share_of_agent_id_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY closed_at DESC, id))) / 60, 1) AS minutes_since_prev_opened_at_2,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY priority, channel ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(AVG(requester_id) OVER (PARTITION BY requester_id_tier, agent_id), 2) AS requester_id_tier_agent_id_avg_requester_id,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY channel, MONTH(closed_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at
    FROM recent
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_requester_id_tier_agent_id AS (PARTITION BY requester_id_tier, agent_id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        requester_id_tier,
        csat_change,
        csat_share_of_requester_id_tier_agent_id,
        distinct_channel_in_requester_id_tier_agent_id,
        minutes_since_prev_opened_at,
        agent_id_csat_avg_csat,
        requester_id_share_of_agent_id_csat,
        minutes_since_prev_opened_at_2,
        minutes_since_prev_closed_at,
        requester_id_tier_agent_id_avg_requester_id,
        second_opened_at
    FROM sequenced
    WHERE csat_seq = 1
      AND requester_id > 7
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    tags,
    distinct_channel_in_requester_id_tier_agent_id,
    agent_id_csat_avg_csat,
    requester_id_share_of_agent_id_csat,
    minutes_since_prev_opened_at_2,
    minutes_since_prev_closed_at,
    second_opened_at,
    LOWER(TRIM(requester_id_tier)) AS requester_id_tier_clean,
    CONCAT_WS(' / ', channel, requester_id_tier) AS channel_requester_id_tier_label
FROM top_rows
ORDER BY id DESC
