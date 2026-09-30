WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE ticket_id <= 58
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM base s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE TO_DATE(t.event_at) < DATE '2025-03-08'
              AND t.event_type = 'reopened'
        )
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_ref,
        s.actor_event_type_label,
        s.event_at_week,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        su.agent_name
    FROM screened s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_csat,
    actor,
    MIN(event_at_week) AS first_event_at_week,
    ROUND(STDDEV(ticket_requester_id), 2) AS stddev_ticket_requester_id
FROM with_details_ticket
GROUP BY ROLLUP(ticket_csat, actor)
