WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 09:00:00'
      AND channel = 'phone'
),
joined_agent AS (
    -- Join support_agents
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(requester_id) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority ORDER BY opened_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_7,
        ROUND(PERCENT_RANK() OVER (ORDER BY opened_at), 3) AS opened_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY agent_id, priority ORDER BY opened_at ASC) AS opened_at_dense_rank,
        LAST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY agent_id, priority ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id,
        MAX(requester_id) OVER w_priority_agent_team AS priority_agent_team_max_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id, priority ORDER BY opened_at ASC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        LAST_VALUE(requester_id) OVER (PARTITION BY agent_team, agent_id ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        SUM(requester_id) OVER (PARTITION BY priority, agent_team) AS priority_agent_team_sum_requester_id,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_team, agent_id ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_2
    FROM joined_agent
    WINDOW w_priority_agent_team AS (PARTITION BY priority, agent_team)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        agent_name,
        agent_team,
        requester_id_avg_7,
        opened_at_dense_rank,
        last_known_requester_id,
        priority_agent_team_max_requester_id,
        last_requester_id,
        priority_agent_team_sum_requester_id,
        first_requester_id_2,
        LOWER(REGEXP_REPLACE(TRIM(agent_team), ' ', '_')) AS agent_team_slug
    FROM ranked
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    agent_name,
    requester_id_avg_7,
    last_known_requester_id,
    priority_agent_team_max_requester_id,
    last_requester_id,
    priority_agent_team_sum_requester_id,
    first_requester_id_2
FROM enriched
ORDER BY last_requester_id DESC, opened_at NULLS LAST, id
LIMIT 10
