WITH valid_tickets AS (
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
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'refund') OR requester_id > 8)
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROUND(AVG(csat) OVER (PARTITION BY agent_team, priority ORDER BY csat ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_team, priority ORDER BY csat ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        DENSE_RANK() OVER w_agent_team_priority_by_csat_3 AS csat_dense_rank,
        RANK() OVER (PARTITION BY agent_team, priority ORDER BY csat ASC NULLS LAST, id) AS csat_rank
    FROM with_details_agent
    WINDOW w_agent_team_priority_by_csat_3 AS (PARTITION BY agent_team, priority ORDER BY csat ASC NULLS LAST, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN priority IN ('P2', 'P4') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM with_trends
)
SELECT id, metric_name, value
FROM (
    SELECT id, csat_dense_rank, distinct_priority_in_agent_id
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (csat_dense_rank, distinct_priority_in_agent_id)
) unp
ORDER BY id, metric_name
