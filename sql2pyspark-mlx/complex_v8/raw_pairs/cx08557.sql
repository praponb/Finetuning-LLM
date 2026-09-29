WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM tickets
    WHERE priority IS NOT NULL
      AND requester_id <= 22
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        priority_prefix,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority), 2) AS agent_id_priority_avg_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_opened_at_to_date_priority_b)) / 60, 1) AS minutes_since_prev_opened_at,
        LAG(requester_id) OVER w_opened_at_to_date_priority_b AS prev_requester_id,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY priority ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        ROUND(PERCENT_RANK() OVER (ORDER BY opened_at DESC NULLS FIRST), 3) AS opened_at_percent_rank,
        LEAD(requester_id) OVER w_all_by_opened_at AS next_requester_id,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at_2,
        DENSE_RANK() OVER w_all_by_requester_id_ties AS requester_id_dense_rank,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_2,
        MIN(requester_id) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_2
    FROM valid_tickets
    WINDOW w_opened_at_to_date_priority_b AS (PARTITION BY TO_DATE(opened_at), priority ORDER BY requester_id, id), w_all_by_opened_at AS (ORDER BY opened_at DESC NULLS FIRST, id), w_all_by_requester_id_ties AS (ORDER BY requester_id DESC)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE requester_id_dense_rank <= 5
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        tags,
        priority_prefix,
        agent_id_priority_avg_requester_id,
        minutes_since_prev_opened_at,
        prev_requester_id,
        first_known_opened_at,
        second_requester_id,
        first_known_opened_at_2,
        requester_id_dense_rank,
        min_requester_id_so_far,
        second_requester_id_2,
        COUNT(*) OVER (ORDER BY opened_at_percent_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(second_requester_id_2) OVER w_first_known_opened_at_to_dat AS first_known_opened_at_to_date_first_known_opened,
        ROUND(AVG(opened_at_percent_rank) OVER (ORDER BY opened_at_percent_rank NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS opened_at_percent_rank_avg_6_prior,
        RANK() OVER (ORDER BY opened_at_percent_rank NULLS LAST) AS opened_at_percent_rank_rank,
        MAX(min_requester_id_so_far) OVER (ORDER BY opened_at_percent_rank NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS min_requester_id_so_far_max_2_prior,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY distinct_agent_id_in_priority, requester_id_dense_rank)) AS distinct_agent_id_in_distinct_agent_id_in_priori
    FROM first_rows
    WINDOW w_first_known_opened_at_to_dat AS (PARTITION BY TO_DATE(first_known_opened_at), YEAR(first_known_opened_at_2))
)
SELECT
    id,
    agent_id,
    opened_at,
    tags,
    priority_prefix,
    agent_id_priority_avg_requester_id,
    minutes_since_prev_opened_at,
    first_known_opened_at_2,
    requester_id_dense_rank,
    min_requester_id_so_far,
    second_requester_id_2,
    running_count,
    first_known_opened_at_to_date_first_known_opened,
    opened_at_percent_rank_avg_6_prior,
    opened_at_percent_rank_rank,
    min_requester_id_so_far_max_2_prior,
    distinct_agent_id_in_distinct_agent_id_in_priori,
    LOWER(REGEXP_REPLACE(TRIM(priority_prefix), ' ', '_')) AS priority_prefix_slug,
    DATE_TRUNC('MONTH', opened_at) AS opened_at_month
FROM sequenced_2
