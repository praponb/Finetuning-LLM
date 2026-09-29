WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
    FROM tickets
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.priority_ref,
        su.agent_name
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        priority_ref,
        agent_name,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        LAST_VALUE(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        LEAD(opened_at) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at DESC NULLS FIRST, id) AS next_opened_at,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at DESC), 3) AS opened_at_percent_rank,
        DENSE_RANK() OVER w_agent_id_by_opened_at_ties_2 AS opened_at_dense_rank,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY MONTH(opened_at), agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at_2
    FROM with_details
    WINDOW w_agent_id_by_opened_at_ties_2 AS (PARTITION BY agent_id ORDER BY opened_at)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        priority_ref,
        agent_name,
        second_opened_at,
        last_known_opened_at,
        next_opened_at,
        minutes_since_prev_opened_at,
        opened_at_percent_rank,
        opened_at_dense_rank,
        last_known_opened_at_2,
        CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref,
        DATEDIFF(DATE '2025-12-31', next_opened_at) AS days_since_next_opened_at
    FROM with_trends
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        agent_name,
        last_known_opened_at,
        last_known_opened_at_2,
        MAX(opened_at_dense_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_opened_at)) AS last_known_opened_at_month_max_opened_at_dense_r,
        ROUND(opened_at_percent_rank - AVG(opened_at_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_opened_at)), 2) AS opened_at_percent_rank_vs_last_known_opened_at_m,
        RANK() OVER (PARTITION BY YEAR(next_opened_at) ORDER BY next_opened_at, id) AS next_opened_at_rank,
        LAG(last_known_opened_at_2) OVER (PARTITION BY agent_id ORDER BY second_opened_at DESC, id) AS prev_last_known_opened_at_2
    FROM labeled
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(prev_last_known_opened_at_2) AS prev_last_known_opened_at_2_day,
        DATE_FORMAT(last_known_opened_at, 'yyyy-MM') AS last_known_opened_at_ym,
        COUNT(*) AS cnt,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        SUM(last_known_opened_at_month_max_opened_at_dense_r) AS total_last_known_opened_at_month_max_opened_at_d,
        MAX(next_opened_at_rank) AS max_next_opened_at_rank
    FROM with_trends_2
    GROUP BY TO_DATE(prev_last_known_opened_at_2), DATE_FORMAT(last_known_opened_at, 'yyyy-MM')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        prev_last_known_opened_at_2_day,
        last_known_opened_at_ym,
        cnt,
        distinct_agent_ids,
        max_next_opened_at_rank,
        SUM(distinct_agent_ids) OVER (PARTITION BY max_next_opened_at_rank) AS max_next_opened_at_rank_sum_distinct_agent_ids,
        COUNT(*) OVER (PARTITION BY max_next_opened_at_rank) AS rows_in_max_next_opened_at_rank,
        MIN(distinct_agent_ids) OVER w_max_next_opened_at_rank AS max_next_opened_at_rank_min_distinct_agent_ids
    FROM stats
    WINDOW w_max_next_opened_at_rank AS (PARTITION BY max_next_opened_at_rank)
)
SELECT
    prev_last_known_opened_at_2_day,
    last_known_opened_at_ym,
    distinct_agent_ids,
    max_next_opened_at_rank,
    max_next_opened_at_rank_sum_distinct_agent_ids,
    max_next_opened_at_rank_min_distinct_agent_ids,
    cnt - max_next_opened_at_rank AS cnt_minus_max_next_opened_at_rank
FROM with_windows
WHERE cnt >= 2
ORDER BY cnt_minus_max_next_opened_at_rank
