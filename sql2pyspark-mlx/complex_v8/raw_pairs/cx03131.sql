WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        CASE WHEN priority IN ('P1', 'P4') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
      AND channel NOT IN ('web', 'phone')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(requester_id_filled) OVER (PARTITION BY agent_id, channel ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_filled,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, agent_id), 0), 4) AS csat_share_of_csat_agent_id,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id_filled, id), 3) AS requester_id_filled_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST), 3) AS csat_cume_dist,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        NTH_VALUE(opened_at, 2) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        MAX(csat) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, channel ORDER BY csat), 3) AS csat_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY csat, id) AS seq_no,
        SUM(csat) OVER (ORDER BY requester_id_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS csat_sum_7,
        NTILE(5) OVER w_agent_id_by_csat AS csat_quintile
    FROM prepared
    WINDOW w_agent_id_by_csat AS (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE seq_no = 1
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    csat,
    tags,
    requester_id_filled,
    priority_group,
    first_requester_id_filled,
    csat_cume_dist,
    max_csat_so_far,
    csat_cume_dist_2,
    seq_no,
    csat_sum_7,
    CONCAT(channel, '#', CAST(seq_no AS STRING)) AS channel_ref,
    LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
FROM top_per_group
ORDER BY channel
