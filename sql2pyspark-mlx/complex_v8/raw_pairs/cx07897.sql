WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
      AND actor IN ('agent', 'system')
),
latest_per_key AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id DESC) AS rn
        FROM base_events
    ) ranked
    WHERE rn = 1
)
SELECT DISTINCT actor, event_type
FROM latest_per_key
ORDER BY event_type DESC
