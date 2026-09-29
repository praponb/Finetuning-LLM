WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE csat BETWEEN 2 AND 4
      AND channel = 'email'
),
stats AS (
    -- Group-level totals
    SELECT
        agent_id,
        priority,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN priority = 'P4' THEN 1 END) / COUNT(*), 1) AS pct_priority_p4,
        COUNT(*) FILTER (WHERE priority = 'P2') AS n_priority_p2,
        COUNT(DISTINCT priority) AS distinct_prioritys,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids
    FROM recent_tickets
    GROUP BY agent_id, priority, DATE_TRUNC('MONTH', opened_at)
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.agent_id,
        s.priority,
        s.opened_at_month,
        s.pct_priority_p4,
        s.n_priority_p2,
        s.distinct_prioritys,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM stats s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team = 'Tech'
),
scored AS (
    -- Add calculated columns
    SELECT *, CONCAT(agent_name, '#', CAST(agent_id AS STRING)) AS agent_name_ref
    FROM combined_details_agent
)
-- Final projection
SELECT
    agent_id,
    priority,
    opened_at_month,
    pct_priority_p4,
    distinct_prioritys,
    agent_team,
    agent_hired_on,
    agent_name,
    agent_name_ref
FROM scored
WHERE agent_team = 'Onboarding'
