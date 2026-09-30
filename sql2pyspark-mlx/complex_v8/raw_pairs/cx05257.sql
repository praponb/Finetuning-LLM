WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
      AND event_at < TIMESTAMP '2025-02-24 12:00:00'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM cleaned_events
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'resolved'
              AND t.event_at < TIMESTAMP '2025-03-02 15:00:00'
        )
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    last_event_at,
    minutes_since_prev_event_at,
    first_known_event_at,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
FROM without_matches
