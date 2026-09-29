WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
metrics AS (
    -- Previous / next values per key
    SELECT *, RANK() OVER w_actor_by_event_at_ties AS event_at_rank
    FROM cleaned_events
    WINDOW w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at)
)
-- Final projection
SELECT
    id,
    event_type,
    SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
    HOUR(event_at) AS event_at_hour
FROM metrics
ORDER BY actor_prefix
