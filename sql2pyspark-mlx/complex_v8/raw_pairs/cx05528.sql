WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE (requester_id < 19 OR opened_at >= TIMESTAMP '2025-01-13 09:00:00')
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags,
        s.priority_len,
        su.tier AS agent_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(priority_len - AVG(priority_len) OVER (PARTITION BY priority), 2) AS priority_len_vs_priority_avg,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), priority_len ORDER BY opened_at DESC) AS opened_at_dense_rank,
        MAX(priority_len) OVER (PARTITION BY priority, agent_tier) AS priority_agent_tier_max_priority_len,
        ROUND(PERCENT_RANK() OVER (ORDER BY priority_len ASC NULLS LAST, id), 3) AS priority_len_percent_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_priority_len_ties, 3) AS priority_len_percent_rank_2,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        ROUND(AVG(priority_len) OVER (ORDER BY priority_len ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS priority_len_avg_6_prior,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), priority_len ORDER BY opened_at DESC) AS opened_at_rank,
        ROUND(AVG(priority_len) OVER (PARTITION BY priority_len, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS priority_len_avg_2_prior,
        MIN(priority_len) OVER (PARTITION BY priority) AS priority_min_priority_len,
        LEAD(priority_len, 3) OVER w_priority_len_agent_id_by_ope_2 AS next3_priority_len
    FROM with_parent_agent
    WINDOW w_all_by_priority_len_ties AS (ORDER BY priority_len ASC NULLS LAST), w_priority_len_agent_id_by_ope_2 AS (PARTITION BY priority_len, agent_id ORDER BY opened_at DESC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags,
        priority_len,
        agent_tier,
        agent_name,
        agent_hired_on,
        priority_len_vs_priority_avg,
        opened_at_dense_rank,
        priority_agent_tier_max_priority_len,
        priority_len_percent_rank,
        priority_len_percent_rank_2,
        rows_in_priority,
        priority_len_avg_6_prior,
        priority_len_avg_2_prior,
        priority_min_priority_len,
        next3_priority_len
    FROM with_windows
    WHERE opened_at_rank <= 5
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        priority,
        tags,
        priority_len,
        agent_name,
        agent_hired_on,
        opened_at_dense_rank,
        priority_len_percent_rank,
        rows_in_priority,
        priority_min_priority_len,
        ROUND(AVG(priority_len) OVER (PARTITION BY YEAR(agent_hired_on), opened_at_dense_rank ORDER BY priority_len_avg_2_prior DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS priority_len_avg_2,
        MAX(priority_len_avg_2_prior) OVER (PARTITION BY next3_priority_len) AS next3_priority_len_max_priority_len_avg_2_prior,
        LAST_VALUE(priority_len) IGNORE NULLS OVER (PARTITION BY priority, agent_tier ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_priority_len
    FROM first_rows
)
SELECT
    id,
    priority,
    priority_len,
    agent_hired_on,
    opened_at_dense_rank,
    priority_len_percent_rank,
    rows_in_priority,
    priority_len_avg_2,
    next3_priority_len_max_priority_len_avg_2_prior,
    last_known_priority_len,
    LENGTH(TRIM(agent_name)) AS agent_name_len,
    SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix
FROM with_trends
ORDER BY agent_name_len DESC, last_known_priority_len DESC NULLS FIRST, id
LIMIT 20
