WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 7
      AND actor IN ('system', 'customer')
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        ticket_requester_id,
        ticket_tags,
        ticket_closed_at,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        LEAD(ticket_closed_at) OVER (PARTITION BY TO_DATE(ticket_closed_at), ticket_id ORDER BY ticket_requester_id DESC, id) AS next_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        SUM(ticket_requester_id) OVER (PARTITION BY event_type) AS event_type_sum_ticket_requester_id,
        LAG(event_at) OVER (ORDER BY ticket_requester_id DESC, id) AS prev_event_at,
        LEAD(ticket_closed_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS next_ticket_closed_at_2,
        ROW_NUMBER() OVER (ORDER BY ticket_requester_id DESC, id) AS ticket_requester_id_seq,
        COUNT(*) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(ticket_requester_id) OVER w_event_type AS event_type_min_ticket_requester_id
    FROM enriched_ticket
    WINDOW w_event_type AS (PARTITION BY event_type)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_tags,
        ticket_closed_at,
        rows_in_event_type,
        next_ticket_closed_at,
        second_ticket_closed_at,
        event_type_sum_ticket_requester_id,
        ticket_requester_id_seq,
        running_count,
        event_type_min_ticket_requester_id,
        DATE_FORMAT(next_ticket_closed_at, 'MMM yyyy') AS next_ticket_closed_at_label
    FROM metrics
)
SELECT
    id,
    ticket_tags,
    rows_in_event_type,
    next_ticket_closed_at,
    running_count,
    event_type_min_ticket_requester_id,
    next_ticket_closed_at_label
FROM derived
WHERE ticket_closed_at < TIMESTAMP '2025-02-24 13:00:00'
