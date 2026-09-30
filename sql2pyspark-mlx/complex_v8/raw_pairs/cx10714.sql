WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        ARRAY_CONTAINS(tags, 'bug') AS has_bug
    FROM tickets
    WHERE agent_id <= 4
),
screened AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier <> 'T3'
              AND UPPER(TRIM(su.agent_name)) LIKE 'I%'
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        priority,
        SUM(CASE WHEN priority IN ('P3', 'P1', 'P4') THEN 1 ELSE 0 END) AS n_priority_in_p3_p1_p4,
        MAX(opened_at) AS last_opened_at,
        MAX(requester_id) AS max_requester_id,
        SUM(requester_id) AS total_requester_id
    FROM screened
    GROUP BY TO_DATE(opened_at), priority
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_opened_at, n_priority_in_p3_p1_p4, priority, total_requester_id
    FROM summary
) src
PIVOT (
    SUM(total_requester_id)
    FOR priority IN ('P1', 'P2', 'P4', 'P3')
)
ORDER BY last_opened_at, n_priority_in_p3_p1_p4
