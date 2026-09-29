WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND actor = 'system'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_clean,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_details_ticket
    WHERE ticket_requester_id > (SELECT AVG(ticket_requester_id) FROM with_details_ticket)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(ticket_requester_id, 0) AS ticket_requester_id_filled,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        TRUNC(agent_hired_on, 'MM') AS agent_hired_on_month_start
    FROM screened
    WHERE LOWER(agent_name) LIKE '%ov%'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, actor, event_type, ticket_requester_id
    FROM annotated
) src
PIVOT (
    MIN(ticket_requester_id)
    FOR event_type IN ('created', 'escalated')
)
