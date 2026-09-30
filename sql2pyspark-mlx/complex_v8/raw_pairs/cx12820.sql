WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        DATE_TRUNC('WEEK', opened_at) AS opened_at_week
    FROM tickets
    WHERE priority IN ('P4', 'P3', 'P2')
),
summary AS (
    -- Summarise per group
    SELECT
        agent_id,
        priority,
        SUM(least_csat_requester_id) AS total_least_csat_requester_id,
        COUNT(*) AS row_count,
        MAX(opened_at_week) AS last_opened_at_week,
        COUNT(*) FILTER (WHERE priority = 'P1') AS n_priority_p1,
        ROUND(AVG(least_csat_requester_id), 2) AS avg_least_csat_requester_id,
        MIN(least_csat_requester_id) AS min_least_csat_requester_id
    FROM base_tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
    GROUP BY agent_id, priority
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER w_priority) AS distinct_agent_id_in_priority,
        NTILE(3) OVER w_all_by_n_priority_p1 AS n_priority_p1_tercile,
        NTH_VALUE(row_count, 2) OVER (ORDER BY last_opened_at_week DESC, agent_id, priority ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        ROUND(avg_least_csat_requester_id - AVG(avg_least_csat_requester_id) OVER (PARTITION BY priority), 2) AS avg_least_csat_requester_id_vs_priority_avg,
        ROUND(AVG(min_least_csat_requester_id) OVER (ORDER BY n_priority_p1, agent_id, priority ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS min_least_csat_requester_id_avg_7
    FROM summary
    WINDOW w_priority AS (PARTITION BY priority), w_all_by_n_priority_p1 AS (ORDER BY n_priority_p1, agent_id, priority)
)
SELECT *
FROM (
    SELECT n_priority_p1_tercile, priority, distinct_agent_id_in_priority
    FROM with_windows
) src
PIVOT (
    SUM(distinct_agent_id_in_priority)
    FOR priority IN ('P4', 'P3')
)
ORDER BY n_priority_p1_tercile
