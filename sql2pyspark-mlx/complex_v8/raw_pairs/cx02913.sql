WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE event_type IN ('reopened', 'replied')
),
latest AS (
    -- Keep the earliest row per ticket_id, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_at_plus_15m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at_plus_15m, id) AS row_num
        FROM cleaned_events
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_prefix,
    event_at_plus_15m,
    LENGTH(TRIM(event_type)) AS event_type_len,
    CONCAT(actor_prefix, '#', CAST(id AS STRING)) AS actor_prefix_ref
FROM latest
WHERE actor = 'customer'
ORDER BY event_type_len DESC
