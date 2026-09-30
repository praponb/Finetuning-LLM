WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 09:00:00'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        ROUND(csat / NULLIF(SUM(csat) OVER w_csat_channel, 0), 4) AS csat_share_of_csat_channel,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id), 0), 4) AS csat_share_of_agent_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, channel), 2) AS csat_vs_csat_channel_avg,
        MIN(csat) OVER (PARTITION BY agent_id) AS agent_id_min_csat,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat, id) AS csat_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, priority ORDER BY csat DESC, id), 3) AS csat_percent_rank,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_csat_ties, 3) AS csat_percent_rank_2,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        RANK() OVER w_csat_priority_by_csat AS csat_rank_2,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        ROUND(csat - AVG(csat) OVER w_agent_id, 2) AS csat_vs_agent_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY csat), 3) AS csat_percent_rank_3,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat DESC) AS csat_rank_3
    FROM prepared
    WINDOW w_csat_channel AS (PARTITION BY csat, channel), w_agent_id AS (PARTITION BY agent_id), w_agent_id_by_csat_ties AS (PARTITION BY agent_id ORDER BY csat DESC), w_csat_priority_by_csat AS (PARTITION BY csat, priority ORDER BY csat DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(csat) OVER (ORDER BY csat_percent_rank DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        MAX(csat_share_of_csat_channel) OVER (ORDER BY csat_percent_rank DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_share_of_csat_channel_max_6_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY second_csat ASC, id), 3) AS second_csat_percent_rank,
        last_known_csat - LAG(last_known_csat) OVER w_all_by_second_csat AS last_known_csat_change,
        ROW_NUMBER() OVER (PARTITION BY agent_id, csat_rank ORDER BY last_known_csat DESC, id) AS seq_no,
        LAG(csat_rank, 3, 0) OVER w_all_by_second_csat AS prev3_csat_rank,
        FIRST_VALUE(csat_vs_agent_id_avg) IGNORE NULLS OVER (ORDER BY second_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_vs_agent_id_avg,
        ROUND(csat_percent_rank / NULLIF(SUM(csat_percent_rank) OVER (PARTITION BY csat_rank_3), 0), 4) AS csat_percent_rank_share_of_csat_rank_3
    FROM ranked
    WINDOW w_all_by_second_csat AS (ORDER BY second_csat ASC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        last_known_csat_change,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        MAX(csat_avg_2_prior) AS max_csat_avg_2_prior,
        COUNT(*) AS cnt,
        SUM(CASE WHEN priority NOT IN ('P2', 'P1') THEN 1 ELSE 0 END) AS n_priority_not_in_p2_p1,
        ROUND(AVG(csat), 2) AS avg_csat
    FROM metrics
    GROUP BY last_known_csat_change
)
-- Final projection
SELECT last_known_csat_change, cnt, ROUND(avg_csat * 1.5, 2) AS avg_csat_adj
FROM stats
ORDER BY avg_csat_adj DESC
