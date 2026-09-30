WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE agent_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY csat NULLS LAST, id), 3) AS csat_cume_dist,
        SUM(csat) OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROUND(CUME_DIST() OVER w_priority_csat_by_closed_at, 3) AS closed_at_cume_dist,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        ROUND(csat_per_requester_id - AVG(csat_per_requester_id) OVER (PARTITION BY agent_id, priority), 2) AS csat_per_requester_id_vs_agent_id_priority_avg,
        LEAD(csat_per_requester_id, 3) OVER (PARTITION BY priority, MONTH(closed_at) ORDER BY csat DESC, id) AS next3_csat_per_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat), 0), 4) AS csat_share_of_csat,
        DENSE_RANK() OVER w_agent_id_by_closed_at_ties AS closed_at_dense_rank,
        csat_per_requester_id - LAG(csat_per_requester_id) OVER (PARTITION BY priority, csat ORDER BY closed_at DESC NULLS FIRST, id) AS csat_per_requester_id_change,
        MIN(csat) OVER w_agent_id_priority AS agent_id_priority_min_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, priority ORDER BY csat_per_requester_id ASC), 3) AS csat_per_requester_id_cume_dist
    FROM cleaned_tickets
    WINDOW w_priority_csat_by_closed_at AS (PARTITION BY priority, csat ORDER BY closed_at DESC NULLS FIRST, id), w_agent_id_by_closed_at_ties AS (PARTITION BY agent_id ORDER BY closed_at DESC), w_agent_id_priority AS (PARTITION BY agent_id, priority)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        csat_per_requester_id,
        csat_cume_dist,
        running_csat,
        closed_at_cume_dist,
        rows_in_csat,
        csat_per_requester_id_vs_agent_id_priority_avg,
        next3_csat_per_requester_id,
        distinct_agent_id_in_csat,
        csat_share_of_csat,
        csat_per_requester_id_change,
        agent_id_priority_min_csat,
        csat_per_requester_id_cume_dist
    FROM windowed
    WHERE closed_at_dense_rank = 1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(csat) OVER (ORDER BY csat_per_requester_id_vs_agent_id_priority_avg ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_max_2_prior,
        NTH_VALUE(csat_share_of_csat, 2) OVER (ORDER BY csat_per_requester_id_vs_agent_id_priority_avg ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_share_of_csat,
        SUM(rows_in_csat) OVER (PARTITION BY csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows_in_csat,
        SUM(agent_id_priority_min_csat) OVER (PARTITION BY priority, distinct_agent_id_in_csat) AS priority_distinct_agent_id_in_csat_sum_agent_id_
    FROM leaders
)
SELECT
    id,
    agent_id,
    priority,
    csat,
    tags,
    csat_per_requester_id,
    running_csat,
    closed_at_cume_dist,
    csat_per_requester_id_vs_agent_id_priority_avg,
    next3_csat_per_requester_id,
    distinct_agent_id_in_csat,
    csat_share_of_csat,
    csat_per_requester_id_change,
    csat_per_requester_id_cume_dist,
    priority_distinct_agent_id_in_csat_sum_agent_id_
FROM sequenced
ORDER BY distinct_agent_id_in_csat NULLS LAST, agent_id NULLS LAST
