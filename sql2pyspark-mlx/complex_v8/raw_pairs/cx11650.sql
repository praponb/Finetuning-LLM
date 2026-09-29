WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        LENGTH(TRIM(priority)) AS priority_len_2
    FROM tickets
    WHERE channel = 'chat'
      AND MONTH(opened_at) IN (1, 2)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        s.priority_len_2,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.agent_name
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        agent_hired_on,
        agent_name,
        RANK() OVER w_all_by_agent_hired_on AS agent_hired_on_rank,
        FIRST_VALUE(agent_hired_on) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        MAX(requester_id) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7,
        MAX(requester_id) OVER (PARTITION BY csat) AS csat_max_requester_id,
        LAG(priority_len_2) OVER w_all_by_agent_hired_on AS prev_priority_len_2,
        ROUND(requester_id - AVG(requester_id) OVER w_csat, 2) AS requester_id_vs_csat_avg,
        NTILE(5) OVER (ORDER BY csat, id) AS csat_quintile,
        ROUND(CUME_DIST() OVER (ORDER BY csat), 3) AS csat_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(closed_at) ORDER BY csat DESC) AS csat_dense_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat,
        ROUND(AVG(csat) OVER (ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS csat_avg_4,
        MIN(requester_id) OVER w_csat AS csat_min_requester_id,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat
    FROM joined
    WINDOW w_all_by_agent_hired_on AS (ORDER BY agent_hired_on, id), w_csat AS (PARTITION BY csat)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        closed_at,
        agent_hired_on,
        agent_hired_on_rank,
        first_agent_hired_on,
        csat_max_requester_id,
        csat_quintile,
        csat_cume_dist,
        csat_dense_rank,
        distinct_agent_id_in_csat,
        csat_min_requester_id,
        rows_in_csat,
        CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref,
        closed_at + INTERVAL 1 DAYS AS closed_at_plus_1d,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug
    FROM with_trends
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(rows_in_csat) OVER (ORDER BY closed_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS rows_in_csat_avg_3_prior,
        LEAD(csat_min_requester_id, 3) OVER (PARTITION BY agent_hired_on_rank ORDER BY csat_max_requester_id DESC, id) AS next3_csat_min_requester_id,
        LEAD(requester_id, 3) OVER (ORDER BY closed_at ASC, id) AS next3_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_hired_on_rank ORDER BY csat_max_requester_id DESC, id), 3) AS csat_max_requester_id_cume_dist,
        FIRST_VALUE(csat_dense_rank) IGNORE NULLS OVER (ORDER BY closed_at_plus_1d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_dense_rank,
        LAG(csat_dense_rank) OVER w_agent_hired_on_rank_by_csat AS prev_csat_dense_rank,
        ROUND(rows_in_csat - AVG(rows_in_csat) OVER (PARTITION BY agent_hired_on_rank), 2) AS rows_in_csat_vs_agent_hired_on_rank_avg,
        ROUND(csat_cume_dist / NULLIF(SUM(csat_cume_dist) OVER w_agent_hired_on_rank, 0), 4) AS csat_cume_dist_share_of_agent_hired_on_rank
    FROM tagged
    WINDOW w_agent_hired_on_rank_by_csat AS (PARTITION BY agent_hired_on_rank ORDER BY csat_max_requester_id DESC, id), w_agent_hired_on_rank AS (PARTITION BY agent_hired_on_rank)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    agent_hired_on_rank,
    first_agent_hired_on,
    csat_max_requester_id,
    csat_quintile,
    distinct_agent_id_in_csat,
    csat_min_requester_id,
    rows_in_csat,
    closed_at_plus_1d,
    agent_name_slug,
    next3_csat_min_requester_id,
    next3_requester_id,
    csat_max_requester_id_cume_dist,
    first_known_csat_dense_rank,
    prev_csat_dense_rank,
    rows_in_csat_vs_agent_hired_on_rank_avg,
    csat_cume_dist_share_of_agent_hired_on_rank,
    LEAST(next3_requester_id, csat_max_requester_id) AS least_next3_requester_id_csat_max_requester_id,
    DATEDIFF(first_agent_hired_on, agent_hired_on) AS days_agent_hired_on_to_first_agent_hired_on
FROM with_trends_2
WHERE requester_id > 8
ORDER BY days_agent_hired_on_to_first_agent_hired_on
