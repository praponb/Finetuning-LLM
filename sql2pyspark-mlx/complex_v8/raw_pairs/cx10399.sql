WITH valid AS (
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
        ABS(csat - 4) AS csat_distance,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE MONTH(closed_at) IN (2, 3)
      AND agent_id >= 8
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.csat_distance,
        s.channel_len,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY YEAR(opened_at), csat_distance ORDER BY csat_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        SUM(csat_distance) OVER w_csat_distance_priority AS csat_distance_priority_sum_csat_distance,
        NTH_VALUE(csat, 2) OVER (PARTITION BY channel_len ORDER BY channel_len ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROUND(channel_len - AVG(channel_len) OVER (PARTITION BY priority), 2) AS channel_len_vs_priority_avg,
        FIRST_VALUE(csat_distance) IGNORE NULLS OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_distance,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY csat ASC) AS csat_dense_rank,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY csat_distance, priority)) AS distinct_agent_team_in_csat_distance_priority,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY csat ASC) AS csat_rank,
        csat - LAG(csat) OVER w_closed_at_month_by_csat AS csat_change,
        ROUND(channel_len / NULLIF(SUM(channel_len) OVER w_csat_distance_priority, 0), 4) AS channel_len_share_of_csat_distance_priority,
        ROUND(csat_distance / NULLIF(SUM(csat_distance) OVER (PARTITION BY csat_distance, priority), 0), 4) AS csat_distance_share_of_csat_distance_priority
    FROM with_details
    WINDOW w_csat_distance_priority AS (PARTITION BY csat_distance, priority), w_closed_at_month_by_csat AS (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY csat ASC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE csat_rank = 1
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(opened_at), csat_change ORDER BY channel_len_vs_priority_avg ASC), 3) AS channel_len_vs_priority_avg_percent_rank,
        LAG(closed_at) OVER w_csat_dense_rank_agent_tier_b AS prev_closed_at,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY csat, csat_distance ORDER BY csat_distance_share_of_csat_distance_priority, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        LAG(closed_at, 2) OVER w_opened_at_year_csat_change_b_2 AS prev2_closed_at,
        RANK() OVER (PARTITION BY YEAR(opened_at), csat_change ORDER BY channel_len_vs_priority_avg ASC, id) AS channel_len_vs_priority_avg_rank
    FROM top_rows
    WINDOW w_csat_dense_rank_agent_tier_b AS (PARTITION BY csat_dense_rank, agent_tier ORDER BY opened_at, id), w_opened_at_year_csat_change_b_2 AS (PARTITION BY YEAR(opened_at), csat_change ORDER BY channel_len_vs_priority_avg ASC, id)
)
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    channel,
    tags,
    channel_len,
    agent_tier,
    csat_distance_priority_sum_csat_distance,
    second_csat,
    channel_len_vs_priority_avg,
    first_known_csat_distance,
    csat_dense_rank,
    distinct_agent_team_in_csat_distance_priority,
    channel_len_share_of_csat_distance_priority,
    csat_distance_share_of_csat_distance_priority,
    prev2_closed_at,
    channel_len_vs_priority_avg_rank,
    TO_DATE(opened_at) AS opened_at_date,
    LEAST(second_csat, csat_rank) AS least_second_csat_csat_rank
FROM windowed_2
ORDER BY id
LIMIT 5
