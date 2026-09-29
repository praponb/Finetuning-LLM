WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND priority NOT IN ('P1', 'P2')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(requester_id) OVER (ORDER BY csat DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_sum_6_prior,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        DENSE_RANK() OVER w_csat_by_requester_id_ties AS requester_id_dense_rank,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY channel, agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        ROUND(AVG(csat) OVER (ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        COUNT(*) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_channel_agent_id_by_csat_tie AS csat_dense_rank,
        ROUND(requester_id - AVG(requester_id) OVER w_csat_agent_id, 2) AS requester_id_vs_csat_agent_id_avg,
        LAG(csat, 3) OVER w_all_by_requester_id AS prev3_csat,
        MIN(csat) OVER w_channel AS channel_min_csat,
        COUNT(*) OVER w_channel AS rows_in_channel,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, agent_id), 0), 4) AS csat_share_of_csat_agent_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat, agent_id)) AS distinct_channel_in_csat_agent_id,
        ROW_NUMBER() OVER w_all_by_requester_id AS rn,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel, agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id
    FROM prepared_tickets
    WINDOW w_csat_by_requester_id_ties AS (PARTITION BY csat ORDER BY requester_id NULLS LAST), w_channel_agent_id_by_csat_tie AS (PARTITION BY channel, agent_id ORDER BY csat), w_csat_agent_id AS (PARTITION BY csat, agent_id), w_all_by_requester_id AS (ORDER BY requester_id, id), w_channel AS (PARTITION BY channel)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(csat_avg_3_prior * 1.07, 2) AS csat_avg_3_prior_adj,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix_2,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM sequenced
    WHERE csat_share_of_csat_agent_id >= 0.3
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        closed_at,
        csat,
        channel_prefix,
        first_known_requester_id,
        first_known_closed_at,
        last_known_csat,
        running_count,
        csat_dense_rank,
        requester_id_vs_csat_agent_id_avg,
        channel_min_csat,
        rows_in_channel,
        distinct_channel_in_csat_agent_id,
        rn,
        csat_avg_3_prior_adj,
        priority_or_unknown,
        FIRST_VALUE(rows_in_channel) IGNORE NULLS OVER (ORDER BY first_known_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_channel,
        RANK() OVER (ORDER BY first_known_requester_id, id) AS first_known_requester_id_rank,
        SUM(csat_avg_3_prior) OVER w_priority_or_unknown_prev3_cs AS priority_or_unknown_prev3_csat_sum_csat_avg_3_pr,
        NTILE(5) OVER (PARTITION BY requester_id_dense_rank ORDER BY second_requester_id, id) AS second_requester_id_quintile,
        SUM(last_known_csat) OVER (PARTITION BY priority_or_unknown, prev3_csat) AS priority_or_unknown_prev3_csat_sum_last_known_cs,
        DENSE_RANK() OVER (PARTITION BY channel, csat ORDER BY second_requester_id) AS second_requester_id_dense_rank
    FROM scored
    WINDOW w_priority_or_unknown_prev3_cs AS (PARTITION BY priority_or_unknown, prev3_csat)
)
-- Final projection
SELECT
    id,
    closed_at,
    csat,
    channel_prefix,
    first_known_requester_id,
    first_known_closed_at,
    running_count,
    csat_dense_rank,
    requester_id_vs_csat_agent_id_avg,
    channel_min_csat,
    rows_in_channel,
    distinct_channel_in_csat_agent_id,
    rn,
    csat_avg_3_prior_adj,
    priority_or_unknown,
    first_known_rows_in_channel,
    first_known_requester_id_rank,
    CONCAT_WS('-', priority_or_unknown, channel_prefix) AS priority_or_unknown_channel_prefix_label,
    ROUND(csat_avg_3_prior_adj * 1.07, 2) AS csat_avg_3_prior_adj_adj
FROM windowed
WHERE csat_dense_rank < 6
ORDER BY running_count, first_known_closed_at
