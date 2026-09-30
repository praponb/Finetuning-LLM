WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        requester_id + csat AS requester_id_plus_csat,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE priority = 'P4'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        priority,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        COUNT(*) AS cnt,
        MAX(requester_id_plus_csat) AS max_requester_id_plus_csat,
        MIN(requester_id_plus_csat) AS min_requester_id_plus_csat,
        COUNT(DISTINCT id) AS distinct_ids
    FROM base
    GROUP BY 1
    HAVING COUNT(*) >= 3
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY priority), 2) AS distinct_ids_vs_priority_avg,
        MIN(cnt) OVER (PARTITION BY priority) AS priority_min_cnt
    FROM summary
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(distinct_agent_ids / NULLIF(priority_min_cnt, 0), 3) AS distinct_agent_ids_per_priority_min_cnt
    FROM windowed
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        distinct_agent_ids - LAG(distinct_agent_ids) OVER (ORDER BY distinct_ids NULLS LAST, priority) AS distinct_agent_ids_change,
        LAST_VALUE(distinct_agent_ids) IGNORE NULLS OVER (PARTITION BY distinct_ids, cnt ORDER BY min_requester_id_plus_csat DESC, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_agent_ids,
        FIRST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY distinct_ids, cnt ORDER BY min_requester_id_plus_csat DESC, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cnt,
        ROUND(priority_min_cnt - AVG(priority_min_cnt) OVER (PARTITION BY cnt), 2) AS priority_min_cnt_vs_cnt_avg
    FROM labeled
)
SELECT
    priority,
    cnt,
    max_requester_id_plus_csat,
    min_requester_id_plus_csat,
    distinct_ids_vs_priority_avg,
    priority_min_cnt,
    distinct_agent_ids_per_priority_min_cnt,
    distinct_agent_ids_change,
    cnt - first_known_cnt AS cnt_minus_first_known_cnt
FROM with_windows
ORDER BY distinct_agent_ids_per_priority_min_cnt NULLS LAST
