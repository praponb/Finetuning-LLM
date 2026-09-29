WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ROW_NUMBER() OVER w_event_type_by_event_at AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type ORDER BY event_at, id), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER w_actor_event_type_by_event_at AS prev_event_at,
        NTILE(4) OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS event_at_quartile,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2
    FROM recent_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_cume_dist,
        minutes_since_prev_event_at,
        prev_event_at,
        event_at_quartile,
        last_event_at,
        minutes_since_prev_event_at_2
    FROM metrics
    WHERE seq_no = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    minutes_since_prev_event_at,
    prev_event_at,
    last_event_at,
    minutes_since_prev_event_at_2
FROM top_per_group
ORDER BY ticket_id DESC
