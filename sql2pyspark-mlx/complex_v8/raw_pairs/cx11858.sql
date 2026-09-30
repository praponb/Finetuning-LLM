WITH scoped AS (
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
        tags
    FROM tickets
    WHERE requester_id < 23
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id, csat), 2) AS csat_vs_agent_id_csat_avg,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, csat), 0), 4) AS csat_share_of_agent_id_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, csat), 0), 4) AS requester_id_share_of_agent_id_csat,
        ROUND(CUME_DIST() OVER w_channel_by_csat_ties, 3) AS csat_cume_dist,
        SUM(requester_id) OVER (PARTITION BY channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        LAG(requester_id) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_requester_id,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY YEAR(opened_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        MAX(csat) OVER (PARTITION BY channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, csat)) AS distinct_channel_in_agent_id_csat
    FROM scoped
    WINDOW w_channel_by_csat_ties AS (PARTITION BY channel ORDER BY csat DESC), w_agent_id_csat AS (PARTITION BY agent_id, csat)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        csat,
        tags,
        requester_id_share_of_agent_id_csat,
        csat_cume_dist,
        running_requester_id,
        prev_requester_id,
        max_csat_so_far,
        distinct_channel_in_agent_id_csat,
        ROUND(AVG(prev_requester_id) OVER (PARTITION BY YEAR(opened_at), distinct_channel_in_agent_id_csat), 2) AS opened_at_year_distinct_channel_in_agent_id_csat,
        NTH_VALUE(csat_share_of_agent_id_csat, 2) OVER (PARTITION BY YEAR(closed_at), DATE_TRUNC('MONTH', first_known_closed_at) ORDER BY csat_share_of_agent_id_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_share_of_agent_id_csat,
        ROUND(AVG(max_csat_so_far) OVER (PARTITION BY distinct_channel_in_agent_id_csat, TO_DATE(first_known_closed_at) ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS max_csat_so_far_avg_3,
        RANK() OVER w_closed_at_year_first_known_c_2 AS csat_share_of_agent_id_csat_rank,
        RANK() OVER w_csat_distinct_channel_in_age AS closed_at_rank,
        DENSE_RANK() OVER (PARTITION BY csat, distinct_channel_in_agent_id_csat ORDER BY closed_at) AS closed_at_dense_rank,
        NTH_VALUE(max_csat_so_far, 2) OVER (PARTITION BY distinct_channel_in_agent_id_csat, TO_DATE(first_known_closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_csat_so_far
    FROM windowed
    WINDOW w_closed_at_year_first_known_c_2 AS (PARTITION BY YEAR(closed_at), DATE_TRUNC('MONTH', first_known_closed_at) ORDER BY csat_share_of_agent_id_csat DESC), w_csat_distinct_channel_in_age AS (PARTITION BY csat, distinct_channel_in_agent_id_csat ORDER BY closed_at, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    csat,
    tags,
    requester_id_share_of_agent_id_csat,
    csat_cume_dist,
    running_requester_id,
    prev_requester_id,
    max_csat_so_far,
    distinct_channel_in_agent_id_csat,
    opened_at_year_distinct_channel_in_agent_id_csat,
    second_csat_share_of_agent_id_csat,
    max_csat_so_far_avg_3,
    csat_share_of_agent_id_csat_rank,
    closed_at_rank,
    closed_at_dense_rank,
    second_max_csat_so_far,
    closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d
FROM sequenced
ORDER BY closed_at, distinct_channel_in_agent_id_csat, id
LIMIT 20
