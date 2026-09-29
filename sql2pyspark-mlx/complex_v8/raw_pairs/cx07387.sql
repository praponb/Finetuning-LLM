WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 8
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM valid s
    JOIN tickets t ON t.agent_id = s.id AND t.priority = 'P2'
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.agent_name,
        s.ticket_id,
        t.closed_at AS ticket_closed_at_2,
        t.agent_id AS ticket_agent_id
    FROM detail_rows_tickets s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE MONTH(t.closed_at) IN (1, 2)
),
deduped AS (
    -- Keep the latest row per ticket_agent_id
    SELECT
        id,
        agent_name,
        ticket_id,
        ticket_closed_at_2,
        ticket_agent_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_agent_id ORDER BY ticket_closed_at_2 DESC, id DESC, ticket_id DESC) AS row_num
        FROM combined_details_ticket
    ) ranked
    WHERE row_num = 1
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.agent_name,
        s.ticket_id,
        s.ticket_closed_at_2,
        s.ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM deduped s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.closed_at >= TIMESTAMP '2025-01-17 21:00:00'
)
SELECT
    id,
    agent_name,
    ticket_id,
    ticket_agent_id,
    ticket_csat,
    DATE_TRUNC('WEEK', ticket_closed_at_2) AS ticket_closed_at_2_week,
    HOUR(ticket_closed_at_2) AS ticket_closed_at_2_hour
FROM combined_details
ORDER BY ticket_csat, ticket_agent_id DESC
