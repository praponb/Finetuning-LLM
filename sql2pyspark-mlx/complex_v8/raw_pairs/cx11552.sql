WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        CASE WHEN priority IN ('P2', 'P4') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE SIZE(tags) >= 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(requester_id, 3, 0) OVER (PARTITION BY priority_group ORDER BY csat NULLS LAST, id) AS prev3_requester_id,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY priority_group ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        LAG(requester_id, 1, 0) OVER w_priority_group_by_csat AS prev_requester_id
    FROM prepared
    WINDOW w_priority_group_by_csat AS (PARTITION BY priority_group ORDER BY csat NULLS LAST, id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.tier AS agent_tier
    FROM metrics s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, agent_tier, priority_group, csat
    FROM enriched
) src
PIVOT (
    MIN(csat)
    FOR priority_group IN ('group_a', 'group_b', 'other')
)
ORDER BY agent_id, agent_tier
