WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags
    FROM tickets
    WHERE priority IN ('P1', 'P4')
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    agent_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    MAX(requester_id) AS max_requester_id
FROM base_tickets
GROUP BY ROLLUP(priority, agent_id)
ORDER BY priority NULLS LAST, agent_id NULLS LAST
