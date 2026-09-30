WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_type <> 'resolved' OR actor IN ('agent', 'customer'))
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM filtered_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', event_type, event_actor) AS event_type_event_actor_label,
        DATE_FORMAT(event_event_at, 'MMM yyyy') AS event_event_at_label
    FROM exploded_detail_events
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        event_id,
        event_event_at,
        event_actor,
        event_type_event_actor_label,
        DATE_FORMAT(event_event_at, 'yyyy-MM') AS event_event_at_label_2,
        LOWER(REGEXP_REPLACE(TRIM(event_actor), ' ', '_')) AS event_actor_slug,
        DATEDIFF(DATE '2025-06-30', event_event_at) AS days_since_event_event_at
    FROM annotated
),
grouped AS (
    -- Group-level totals
    SELECT
        ticket_id,
        ROUND(AVG(days_since_event_event_at), 2) AS avg_days_since_event_event_at,
        COUNT(*) AS cnt,
        COUNT(DISTINCT event_actor) AS distinct_event_actors,
        SUM(days_since_event_event_at) AS total_days_since_event_event_at,
        SUM(days_since_event_event_at) FILTER (WHERE event_actor = 'agent') AS days_since_event_event_at_event_actor_agent
    FROM scored
    GROUP BY ticket_id
    HAVING AVG(days_since_event_event_at) > 175
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.first_event_at,
        t.first_event_at_2
    FROM grouped s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    ticket_id,
    avg_days_since_event_event_at,
    cnt,
    distinct_event_actors,
    total_days_since_event_event_at,
    days_since_event_event_at_event_actor_agent,
    first_event_at
FROM profiled
WHERE first_event_at_2 >= TIMESTAMP '2025-01-25 09:00:00'
ORDER BY cnt, distinct_event_actors
