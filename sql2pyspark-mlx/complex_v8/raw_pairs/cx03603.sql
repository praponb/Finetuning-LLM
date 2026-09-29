WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at,
        CASE WHEN priority IN ('P1', 'P3') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-03-22'
),
latest AS (
    -- Keep the latest row per priority_group, agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        minutes_closed_at_to_opened_at,
        priority_group,
        requester_id_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority_group, agent_id ORDER BY opened_at DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    priority_group,
    agent_id,
    csat,
    SUM(requester_id_bucket) AS total_requester_id_bucket,
    COUNT(DISTINCT priority_group) AS distinct_priority_groups
FROM latest
GROUP BY CUBE(priority_group, agent_id, csat)
