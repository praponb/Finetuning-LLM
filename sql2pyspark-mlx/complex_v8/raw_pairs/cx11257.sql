WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, agent_id ORDER BY requester_id DESC), 3) AS requester_id_percent_rank,
        DENSE_RANK() OVER (PARTITION BY agent_id, csat ORDER BY csat) AS csat_dense_rank,
        LAG(csat, 1, 0) OVER w_agent_id_csat_by_requester_i AS prev_csat,
        FIRST_VALUE(csat) OVER (PARTITION BY csat, agent_id ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        MAX(requester_id) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far,
        LEAD(requester_id) OVER (PARTITION BY csat ORDER BY csat, id) AS next_requester_id,
        MAX(csat) OVER (PARTITION BY agent_id, csat) AS agent_id_csat_max_csat,
        MAX(csat) OVER (PARTITION BY csat, agent_id) AS csat_agent_id_max_csat,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat
    FROM filtered_tickets
    WINDOW w_agent_id_csat_by_requester_i AS (PARTITION BY agent_id, csat ORDER BY requester_id, id)
)
SELECT
    id,
    requester_id,
    requester_id_percent_rank,
    prev_csat,
    first_csat,
    max_requester_id_so_far,
    next_requester_id,
    agent_id_csat_max_csat,
    rows_in_agent_id_csat,
    max_requester_id_so_far + csat_agent_id_max_csat AS max_requester_id_so_far_plus_csat_agent_id_max_c
FROM windowed
ORDER BY max_requester_id_so_far_plus_csat_agent_id_max_c DESC NULLS FIRST, prev_csat DESC NULLS FIRST
