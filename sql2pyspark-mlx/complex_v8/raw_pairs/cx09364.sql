WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE ticket_id >= 22
      AND actor NOT IN ('customer', 'system')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM prepared_events
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    actor,
    MAX(event_at_plus_30m) AS last_event_at_plus_30m
FROM tagged
GROUP BY GROUPING SETS ((event_type, actor), (event_type), ())
