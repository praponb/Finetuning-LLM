WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
    FROM ticket_events
    WHERE event_type = 'escalated'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(actor)) AS actor_len,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM valid_events
),
derived AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM tagged
    WHERE actor_len >= 4
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id BETWEEN 2 AND 19
)
-- Final projection
SELECT
    id,
    event_type,
    event_at_year,
    days_since_event_at,
    ticket_closed_at,
    ticket_requester_id,
    DATEDIFF(DATE '2025-06-30', event_at_year) AS days_since_event_at_year,
    ROUND(actor_len / NULLIF(ticket_requester_id, 0), 3) AS actor_len_per_ticket_requester_id
FROM with_parent_ticket
