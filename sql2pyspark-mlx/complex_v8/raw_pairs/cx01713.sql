WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.agent_name
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id AND MONTH(t.closed_at) IN (2, 3)
    JOIN support_agents su ON su.id = t.agent_id
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority_2,
        t.requester_id AS ticket_requester_id
    FROM with_details s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_actor_label,
        s.ticket_requester_id,
        t.csat AS ticket_csat_2,
        t.tags AS ticket_tags,
        t.agent_id AS ticket_agent_id
    FROM enriched_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
event_type_stats AS (
    -- Avg ticket_csat_2 per event_type
    SELECT event_type, AVG(ticket_csat_2) AS avg_ticket_csat_2
    FROM with_details_ticket
    GROUP BY event_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_csat_2,
        ROUND(s.ticket_csat_2 - g.avg_ticket_csat_2, 2) AS ticket_csat_2_minus_avg
    FROM with_details_ticket s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.ticket_csat_2 > g.avg_ticket_csat_2
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    event_type_actor_label,
    ticket_requester_id,
    ticket_csat_2,
    ticket_tags,
    ticket_agent_id,
    avg_ticket_csat_2,
    UPPER(TRIM(actor)) AS actor_clean
FROM compared
WHERE ticket_csat_2 <= 3
ORDER BY ticket_requester_id, id
LIMIT 10
