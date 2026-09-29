WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'agent'
      AND MONTH(event_at) IN (1, 3)
),
scored AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(event_type)) AS event_type_len
    FROM cleaned
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM scored s
    JOIN tickets t ON t.id = s.ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM with_details s
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        t.agent_id AS ticket_agent_id,
        su.team AS agent_team,
        su.agent_name
    FROM with_counts s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    event_at,
    ticket_requester_id,
    last_event_at_2,
    ticket_closed_at,
    ticket_tags,
    ticket_agent_id,
    agent_team
FROM with_details_ticket
ORDER BY agent_team, id
LIMIT 5
