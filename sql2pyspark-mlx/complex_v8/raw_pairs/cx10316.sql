WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
latest_per_key AS (
    -- Keep the latest row per event_type, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id DESC) AS row_num
        FROM scoped_events
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id), 3) AS event_at_percent_rank,
        NTILE(5) OVER w_event_type_ticket_id_by_even_2 AS event_at_quintile
    FROM latest_per_key
    WINDOW w_event_type_ticket_id_by_even_2 AS (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id)
),
latest AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        first_event_at,
        minutes_since_prev_event_at,
        event_at_percent_rank,
        event_at_quintile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY first_event_at DESC, id DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.minutes_since_prev_event_at,
        s.event_at_percent_rank,
        s.event_at_quintile,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM latest s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-01-13 11:00:00'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_quintile,
    event_event_type,
    event_actor,
    event_event_at,
    ROUND(event_at_percent_rank * 0.25, 2) AS event_at_percent_rank_adj,
    CONCAT(event_actor, '#', CAST(ticket_id AS STRING)) AS event_actor_ref
FROM exploded_detail_events
ORDER BY event_actor DESC
