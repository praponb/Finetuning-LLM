WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 09:00:00'
      AND event_type <> 'reopened'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM prepared
    WHERE ticket_id IS NOT NULL
)
SELECT
    id,
    actor,
    event_at_month,
    event_at_plus_2h,
    CONCAT_WS('-', actor_slug, actor) AS actor_slug_actor_label,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug_2
FROM annotated
WHERE ticket_id >= 9
ORDER BY actor_slug_actor_label NULLS LAST, actor DESC
