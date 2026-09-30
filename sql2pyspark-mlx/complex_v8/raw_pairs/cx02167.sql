WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor <> 'customer'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at >= TIMESTAMP '2025-01-17 21:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        ticket_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id), 3) AS event_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_closed_at DESC, id), 3) AS ticket_closed_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_closed_at DESC), 3) AS ticket_closed_at_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY ticket_id, YEAR(event_at) ORDER BY event_at DESC NULLS FIRST, id) AS event_at_dense_rank_2,
        NTILE(5) OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id) AS ticket_closed_at_quintile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_ticket_closed_at_ties, 3) AS ticket_closed_at_percent_rank,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY actor, event_type ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at
    FROM with_details
    WINDOW w_all_by_ticket_closed_at_ties AS (ORDER BY ticket_closed_at DESC)
)
SELECT
    id,
    actor,
    event_at_percent_rank,
    event_at_dense_rank,
    ticket_closed_at_cume_dist_2,
    event_at_dense_rank_2,
    ticket_closed_at_quintile,
    ticket_closed_at_percent_rank
FROM windowed
WHERE event_at_dense_rank < 8
ORDER BY event_at_dense_rank_2 DESC NULLS FIRST
