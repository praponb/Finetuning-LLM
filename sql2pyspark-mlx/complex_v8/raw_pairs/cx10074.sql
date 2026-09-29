WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        requester_id - csat AS requester_id_minus_csat,
        UPPER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN priority IN ('P4', 'P3') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM scoped_tickets
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(priority_group, 'ALL') AS priority_group,
    agent_id,
    GROUPING_ID() AS gid,
    ROUND(SUM(requester_id_minus_csat) / NULLIF(SUM(csat), 0), 3) AS requester_id_minus_csat_to_csat_ratio,
    ROUND(AVG(csat), 2) AS avg_csat,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
FROM derived
GROUP BY GROUPING SETS ((priority, priority_group, agent_id), (priority), (priority_group), ())
ORDER BY priority NULLS LAST, priority_group NULLS LAST, agent_id NULLS LAST
