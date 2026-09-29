WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(actor, '#', CAST(actor_len AS STRING)) AS actor_ref,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM filtered
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_len,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM scored s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(CASE WHEN actor = 'agent' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM with_children_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT actor_len, event_actor, event_actor_count
    FROM profiled
) src
PIVOT (
    COUNT(event_actor_count)
    FOR event_actor IN ('system', 'customer', 'agent')
)
ORDER BY actor_len
