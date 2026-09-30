WITH prepared AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE event_type <> 'assigned'
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.priority AS ticket_priority
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(10) OVER w_ticket_id_actor_by_event_at AS event_at_decile,
        RANK() OVER w_actor_ticket_id_by_event_at_2 AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at_2,
        LAG(event_at) OVER (ORDER BY event_at ASC, id) AS prev_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC, id), 3) AS event_at_cume_dist,
        LEAD(event_at, 2) OVER w_all_by_event_at AS next2_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_3
    FROM with_parent
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id), w_actor_ticket_id_by_event_at_2 AS (PARTITION BY actor, ticket_id ORDER BY event_at DESC), w_all_by_event_at AS (ORDER BY event_at ASC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    ticket_priority,
    minutes_since_prev_event_at,
    event_at_decile,
    event_at_rank,
    minutes_since_prev_event_at_2,
    prev_event_at,
    event_at_cume_dist,
    next2_event_at,
    ROUND(event_at_cume_dist * 1.07, 2) AS event_at_cume_dist_adj
FROM ranked
ORDER BY event_at_cume_dist, ticket_id
