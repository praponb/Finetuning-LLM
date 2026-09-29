WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-01-25 09:00:00'
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.channel AS ticket_channel, t.csat AS ticket_csat
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_details_ticket s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_slug,
        s.actor_clean,
        s.ticket_channel,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM without_matches s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('resolved', 'created')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_event_at) OVER (PARTITION BY ticket_channel, event_type ORDER BY event_event_at DESC NULLS FIRST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY event_id ORDER BY event_at ASC, id, event_id) AS next3_event_at
    FROM with_children
),
latest AS (
    -- Keep the latest row per ticket_id, event_event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_slug,
        actor_clean,
        ticket_channel,
        event_id,
        event_actor,
        event_event_type,
        event_event_at,
        last_event_event_at,
        first_event_at,
        next3_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_event_type ORDER BY event_event_at DESC, id DESC, event_id DESC) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_slug,
    ticket_channel,
    event_id,
    event_event_type,
    event_event_at,
    last_event_event_at,
    first_event_at,
    next3_event_at,
    UPPER(TRIM(event_type_slug)) AS event_type_slug_clean,
    DATE_TRUNC('HOUR', event_event_at) AS event_event_at_hour
FROM latest
WHERE ticket_id >= 49
ORDER BY event_id DESC, id DESC
