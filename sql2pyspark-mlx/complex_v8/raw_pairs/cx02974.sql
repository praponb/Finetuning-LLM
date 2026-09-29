WITH filtered_tickets AS (
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
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        tags,
        least_csat_requester_id,
        ROUND(least_csat_requester_id / NULLIF(SUM(least_csat_requester_id) OVER w_csat, 0), 4) AS least_csat_requester_id_share_of_csat,
        ROUND(CUME_DIST() OVER w_all_by_csat, 3) AS csat_cume_dist,
        MIN(requester_id) OVER (PARTITION BY csat) AS csat_min_requester_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY csat)) AS distinct_priority_in_csat
    FROM filtered_tickets
    WINDOW w_csat AS (PARTITION BY csat), w_all_by_csat AS (ORDER BY csat DESC NULLS FIRST, id)
)
SELECT
    priority,
    distinct_priority_in_csat,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    COUNT(DISTINCT id) AS distinct_ids
FROM ranked
GROUP BY GROUPING SETS ((priority, distinct_priority_in_csat), (priority))
ORDER BY priority NULLS LAST, distinct_priority_in_csat NULLS LAST
