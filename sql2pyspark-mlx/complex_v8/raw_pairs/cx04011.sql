WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE (TO_DATE(event_at) < DATE '2025-03-02' OR actor = 'customer')
),
latest AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        actor_slug,
        event_type_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM scoped_events
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM latest
    WHERE actor_slug IS NOT NULL
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_prefix,
        s.event_at_dayofweek,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at
    FROM labeled s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.closed_at >= TIMESTAMP '2025-01-15 08:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(event_at_dayofweek - AVG(event_at_dayofweek) OVER w_ticket_id, 2) AS event_at_dayofweek_vs_ticket_id_avg,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY TO_DATE(event_at), event_at_dayofweek ORDER BY event_at_dayofweek, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        LAG(ticket_closed_at, 3) OVER (PARTITION BY TO_DATE(event_at), event_at_dayofweek ORDER BY event_at_dayofweek, id) AS prev3_ticket_closed_at
    FROM combined_details
    WINDOW w_ticket_id AS (PARTITION BY ticket_id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor_prefix,
    event_at_dayofweek,
    ticket_priority,
    ticket_closed_at,
    minutes_since_prev_ticket_closed_at,
    CONCAT(actor_prefix, '#', CAST(id AS STRING)) AS actor_prefix_ref
FROM sequenced
ORDER BY event_at_dayofweek DESC, id
