WITH prepared AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 4
      AND hired_on IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per team
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat
    FROM deduped s
    JOIN tickets t ON t.agent_id = s.id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', ticket_closed_at) AS days_since_ticket_closed_at
    FROM detail_rows_tickets
    WHERE LOWER(agent_name) LIKE '%or%'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('|', agent_name, team) AS agent_name_team_label,
        SUBSTRING(TRIM(agent_name), 1, 4) AS agent_name_prefix
    FROM annotated
    WHERE team <> 'Onboarding'
)
-- Final projection
SELECT
    id,
    agent_name,
    team,
    ticket_id,
    ticket_csat,
    days_since_ticket_closed_at,
    agent_name_team_label,
    agent_name_prefix
FROM enriched
WHERE ticket_id IS NOT NULL
ORDER BY id
