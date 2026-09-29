WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type IN ('escalated', 'created', 'assigned')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at_plus_7d) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_plus_7d,
        NTH_VALUE(event_at_plus_7d, 2) OVER (ORDER BY event_at_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_7d,
        NTILE(5) OVER (ORDER BY event_at_plus_7d, id) AS event_at_plus_7d_quintile,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at), 3) AS event_at_cume_dist,
        LAG(event_at) OVER w_all_by_event_at_plus_7d AS prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type ORDER BY event_at), 3) AS event_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at, id), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (ORDER BY event_at_plus_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id) AS prev_event_at_2,
        RANK() OVER w_ticket_id_actor_by_event_at_2 AS event_at_rank
    FROM base_events
    WINDOW w_all_by_event_at_plus_7d AS (ORDER BY event_at_plus_7d, id), w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_or_unknown,
    second_event_at_plus_7d,
    event_at_cume_dist_2,
    event_at_percent_rank,
    last_event_at,
    prev_event_at_2,
    event_at_rank
FROM metrics
ORDER BY event_at_rank DESC, id
LIMIT 5
