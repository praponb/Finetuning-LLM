WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 6 AND 8
      AND closed_at < TIMESTAMP '2025-02-27 06:00:00'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.priority_or_unknown,
        s.priority_ref,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id
),
enriched_2 AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
    FROM enriched
    WHERE NOT ARRAY_CONTAINS(tags, 'refund')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        priority_or_unknown,
        priority_ref,
        agent_tier,
        agent_team,
        closed_at_label,
        LAST_VALUE(csat) OVER (PARTITION BY csat, priority_or_unknown ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        MAX(csat) OVER w_priority_agent_team AS priority_agent_team_max_csat,
        NTILE(4) OVER (PARTITION BY csat, priority_or_unknown ORDER BY csat, id) AS csat_quartile,
        LEAD(closed_at) OVER (PARTITION BY csat, priority_or_unknown ORDER BY csat, id) AS next_closed_at,
        COUNT(*) OVER w_priority_agent_team AS rows_in_priority_agent_team,
        ROUND(CUME_DIST() OVER (ORDER BY agent_hired_on ASC), 3) AS agent_hired_on_cume_dist
    FROM enriched_2
    WINDOW w_priority_agent_team AS (PARTITION BY priority, agent_team)
)
-- Subtotals with ROLLUP
SELECT
    priority,
    csat,
    GROUPING(priority) AS is_total_priority,
    MAX(agent_hired_on_cume_dist) AS max_agent_hired_on_cume_dist,
    SUM(priority_agent_team_max_csat) AS total_priority_agent_team_max_csat
FROM with_windows
GROUP BY ROLLUP(priority, csat)
ORDER BY priority NULLS LAST, csat NULLS LAST
