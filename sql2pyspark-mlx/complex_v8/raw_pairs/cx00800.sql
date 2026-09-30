WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'reopened') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE event_type <> 'resolved'
      AND ticket_id >= 50
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at
    FROM valid_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.ticket_priority,
        s.ticket_requester_id,
        s.ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM joined s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id IS NOT NULL
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor,
        ticket_priority,
        ticket_requester_id,
        ticket_opened_at,
        ticket_agent_id,
        TO_DATE(ticket_opened_at) AS ticket_opened_at_date
    FROM with_details_ticket
    WHERE ticket_requester_id > 11
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, SUM(csat) AS total_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_csat
    FROM scored s
    LEFT JOIN ticket_stats t ON t.agent_id = s.ticket_agent_id
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    ticket_priority,
    ticket_requester_id,
    ticket_opened_at,
    ticket_opened_at_date,
    total_ticket_csat,
    DATE_TRUNC('MONTH', ticket_opened_at_date) AS ticket_opened_at_date_month,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
FROM with_counts
ORDER BY id DESC, event_type DESC
