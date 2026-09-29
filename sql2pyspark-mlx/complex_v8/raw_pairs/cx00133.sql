WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.closed_at AS ticket_closed_at
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_closed_at,
        ROUND(CUME_DIST() OVER w_ticket_id_by_ticket_closed_a, 3) AS ticket_closed_at_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist,
        LEAD(ticket_closed_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS next2_ticket_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC), 3) AS ticket_closed_at_cume_dist_2,
        LEAD(event_at, 2) OVER w_all_by_event_at AS next2_event_at,
        LAG(ticket_closed_at, 2) OVER w_ticket_id_event_type_by_even AS prev2_ticket_closed_at,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at) AS event_at_rank,
        LAG(ticket_closed_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev_ticket_closed_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_decile,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at
    FROM with_parent_ticket
    WINDOW w_ticket_id_by_ticket_closed_a AS (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id), w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    next2_ticket_closed_at,
    ticket_closed_at_cume_dist_2,
    next2_event_at,
    prev2_ticket_closed_at,
    first_known_ticket_closed_at,
    prev_ticket_closed_at,
    minutes_since_prev_ticket_closed_at,
    DATE_FORMAT(prev2_ticket_closed_at, 'yyyy-MM-dd') AS prev2_ticket_closed_at_label,
    ROUND(event_at_cume_dist / NULLIF(event_at_rank, 0), 3) AS event_at_cume_dist_per_event_at_rank
FROM sequenced
