WITH cleaned AS (
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
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(ticket_closed_at) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_closed_at,
        DENSE_RANK() OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_dense_rank,
        LEAD(ticket_closed_at, 3) OVER w_actor_event_type_by_ticket_c AS next3_ticket_closed_at,
        NTILE(5) OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_quintile,
        NTH_VALUE(ticket_closed_at, 2) OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_closed_at, id), 3) AS ticket_closed_at_percent_rank,
        LEAD(ticket_closed_at) OVER (ORDER BY ticket_closed_at, id) AS next_ticket_closed_at,
        ROW_NUMBER() OVER w_all_by_ticket_closed_at AS rn,
        LAST_VALUE(ticket_closed_at) IGNORE NULLS OVER (ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_closed_at
    FROM combined_details_ticket
    WINDOW w_actor_event_type_by_ticket_c AS (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id), w_all_by_ticket_closed_at AS (ORDER BY ticket_closed_at, id)
)
SELECT
    id,
    ticket_id,
    event_type,
    ticket_tags,
    ticket_closed_at,
    next3_ticket_closed_at,
    ticket_closed_at_quintile,
    ticket_closed_at_percent_rank,
    next_ticket_closed_at,
    rn,
    last_known_ticket_closed_at,
    CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
FROM sequenced
WHERE actor IN ('customer', 'agent')
ORDER BY next3_ticket_closed_at DESC
