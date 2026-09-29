WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id, event_id) AS event_at_seq,
        NTILE(4) OVER (ORDER BY event_at DESC, id, event_id) AS event_at_quartile
    FROM with_children_events
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.first_event_at,
        t.last_event_at_2
    FROM with_trends s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, field, amount
FROM (
    SELECT id, event_id, event_at_seq, event_at_quartile
    FROM profiled
) src
UNPIVOT (
    amount FOR field IN (event_at_seq, event_at_quartile)
) unp
ORDER BY id, event_id, field
