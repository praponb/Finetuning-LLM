WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 8 AND 10
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        csat,
        tags,
        ROUND(csat / NULLIF(SUM(csat) OVER w_agent_id_channel, 0), 4) AS csat_share_of_agent_id_channel,
        ROUND(AVG(csat) OVER (PARTITION BY channel, csat ORDER BY csat DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_6_prior,
        LAST_VALUE(csat) OVER (PARTITION BY channel, agent_id ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        SUM(csat) OVER (PARTITION BY channel, csat ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_sum_2,
        ROUND(csat - AVG(csat) OVER w_agent_id_channel, 2) AS csat_vs_agent_id_channel_avg,
        DENSE_RANK() OVER (PARTITION BY channel, csat ORDER BY csat DESC, id) AS csat_dense_rank
    FROM valid
    WINDOW w_agent_id_channel AS (PARTITION BY agent_id, channel)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        csat_share_of_agent_id_channel,
        ROUND(csat_avg_6_prior * 0.9, 2) AS csat_avg_6_prior_adj,
        csat_avg_6_prior - csat_sum_2 AS csat_avg_6_prior_minus_csat_sum_2,
        ABS(last_csat - 3) AS last_csat_distance
    FROM sequenced
    WHERE csat_dense_rank <= 9
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY csat_avg_6_prior_adj DESC, id) AS csat_avg_6_prior_adj_quartile,
        ROUND(csat_avg_6_prior_adj / NULLIF(SUM(csat_avg_6_prior_adj) OVER (PARTITION BY last_csat_distance), 0), 4) AS csat_avg_6_prior_adj_share_of_last_csat_distance,
        NTH_VALUE(last_csat_distance, 2) OVER (ORDER BY csat_avg_6_prior_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_csat_distance,
        COUNT(*) OVER (PARTITION BY last_csat_distance) AS rows_in_last_csat_distance,
        MAX(csat_avg_6_prior_adj) OVER (PARTITION BY last_csat_distance) AS last_csat_distance_max_csat_avg_6_prior_adj
    FROM enriched
)
-- Final projection
SELECT
    id,
    csat_avg_6_prior_adj,
    csat_avg_6_prior_minus_csat_sum_2,
    last_csat_distance,
    csat_avg_6_prior_adj_quartile,
    rows_in_last_csat_distance,
    last_csat_distance_max_csat_avg_6_prior_adj,
    CASE WHEN csat_avg_6_prior_adj >= 5.2 THEN 'large' WHEN csat_avg_6_prior_adj >= 3.0 THEN 'medium' ELSE 'small' END AS csat_avg_6_prior_adj_tier,
    ROUND(csat_avg_6_prior_adj * 1.07, 2) AS csat_avg_6_prior_adj_adj
FROM windowed
ORDER BY id DESC, csat_avg_6_prior_minus_csat_sum_2 DESC
