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
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 3)
      AND agent_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority), 0), 4) AS csat_share_of_priority,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority), 2) AS requester_id_vs_priority_avg,
        MAX(priority_len) OVER (PARTITION BY csat, channel ORDER BY priority_len DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS priority_len_max_3,
        LAG(priority_len, 3, 0) OVER w_opened_at_month_by_opened_at AS prev3_priority_len,
        MAX(requester_id) OVER w_csat_priority_len AS csat_priority_len_max_requester_id,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY csat, priority_len), 2) AS requester_id_vs_csat_priority_len_avg,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        ROUND(AVG(requester_id) OVER w_csat_priority_len, 2) AS csat_priority_len_avg_requester_id,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY priority, agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        LEAD(opened_at) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id) AS next_opened_at,
        MAX(requester_id) OVER w_priority AS priority_max_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_2,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7
    FROM recent
    WINDOW w_opened_at_month_by_opened_at AS (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at, id), w_csat_priority_len AS (PARTITION BY csat, priority_len), w_priority AS (PARTITION BY priority)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY priority_len ORDER BY priority_len_max_3 DESC, id) AS priority_len_max_3_dense_rank,
        MAX(prev3_priority_len) OVER (PARTITION BY channel) AS channel_max_prev3_priority_len,
        LAG(second_opened_at_2, 2) OVER (PARTITION BY priority, csat ORDER BY csat_priority_len_avg_requester_id, id) AS prev2_second_opened_at_2,
        SUM(priority_len_max_3) OVER w_distinct_agent_id_in_priorit AS distinct_agent_id_in_priority_prev3_priority_len,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel
    FROM sequenced
    WINDOW w_distinct_agent_id_in_priorit AS (PARTITION BY distinct_agent_id_in_priority, prev3_priority_len)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    priority_len,
    csat_share_of_priority,
    requester_id_vs_priority_avg,
    priority_len_max_3,
    first_known_opened_at,
    csat_priority_len_avg_requester_id,
    next_opened_at,
    second_opened_at_2,
    csat_avg_7,
    channel_max_prev3_priority_len,
    distinct_agent_id_in_priority_prev3_priority_len,
    rows_in_channel
FROM with_trends
ORDER BY csat_avg_7 DESC, priority_len_max_3 DESC NULLS FIRST
