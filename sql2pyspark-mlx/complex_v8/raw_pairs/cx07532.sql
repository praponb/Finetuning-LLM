WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN priority IN ('P3', 'P1') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.priority_group,
        su.agent_name,
        su.team AS agent_team
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(priority_group, '#', CAST(id AS STRING)) AS priority_group_ref,
        LOWER(TRIM(agent_name)) AS agent_name_clean
    FROM with_parent
)
SELECT
    id,
    agent_id,
    priority_group,
    agent_name,
    agent_team,
    agent_name_clean,
    LOWER(REGEXP_REPLACE(TRIM(agent_name_clean), ' ', '_')) AS agent_name_clean_slug
FROM annotated
ORDER BY priority_group, agent_id DESC
