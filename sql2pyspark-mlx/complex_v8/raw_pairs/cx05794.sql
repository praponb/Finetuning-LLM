WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        LOWER(TRIM(event_type)) AS event_type_clean,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND event_type IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at_week, 3) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS next3_event_at_week,
        LEAD(event_at, 3) OVER (PARTITION BY actor ORDER BY event_at_week DESC, id) AS next3_event_at
    FROM filtered_events
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_prefix,
        s.event_type_clean,
        s.event_at_week,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.opened_at >= TIMESTAMP '2025-01-31 09:00:00'
),
deduped AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_prefix,
        event_type_clean,
        event_at_week,
        ticket_priority,
        ticket_csat,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM with_parent
    ) ranked
    WHERE recency_rank = 1
),
latest_per_key AS (
    -- Keep the latest row per ticket_priority
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_prefix,
        event_type_clean,
        event_at_week,
        ticket_priority,
        ticket_csat,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_priority ORDER BY event_at DESC, id DESC) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_prefix,
    event_type_clean,
    event_at_week,
    ticket_priority,
    ticket_csat,
    ticket_channel,
    event_at_week + INTERVAL 7 DAYS AS event_at_week_plus_7d
FROM latest_per_key
ORDER BY id DESC, ticket_csat NULLS LAST
