WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour,
        CASE WHEN event_type IN ('assigned', 'replied') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 12:00:00'
      AND event_type NOT IN ('assigned', 'escalated')
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_hour,
        s.event_type_group,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel,
        t.opened_at AS ticket_opened_at
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id > 3
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM with_details s
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_group,
        s.ticket_closed_at,
        s.ticket_channel,
        s.ticket_opened_at,
        s.first_event_at,
        t.closed_at AS ticket_closed_at_2,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        su.hired_on AS agent_hired_on
    FROM with_counts s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id <= 18
)
-- Final projection
SELECT
    id,
    actor,
    event_type_group,
    ticket_channel,
    first_event_at,
    ticket_closed_at_2,
    ticket_requester_id,
    ticket_priority,
    agent_hired_on,
    ABS(ticket_requester_id - 7) AS ticket_requester_id_distance
FROM enriched_ticket
WHERE ticket_channel IN ('web', 'email', 'phone')
ORDER BY event_type_group DESC
