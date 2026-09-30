WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(TRIM(actor)) AS actor_clean,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE event_type = 'replied'
),
stats AS (
    -- Summarise per group
    SELECT
        event_type,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        ticket_id,
        COUNT(*) FILTER (WHERE actor IN ('system', 'agent')) AS n_actor_in_system_agent,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM base_events
    GROUP BY event_type, DATE_FORMAT(event_at, 'yyyy-MM'), ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM stats s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_children AS (
    -- One row per event
    SELECT
        s.event_type,
        s.event_at_ym,
        s.ticket_id,
        s.n_actor_in_system_agent,
        s.n_rows,
        s.distinct_ids,
        s.distinct_actors,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM with_counts s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
scored AS (
    -- Add calculated columns
    SELECT
        event_type,
        event_at_ym,
        ticket_id,
        n_actor_in_system_agent,
        n_rows,
        event_id,
        event_actor,
        CASE WHEN event_actor IN ('agent', 'system') THEN 'group_a' WHEN event_actor = 'customer' THEN 'group_b' ELSE 'other' END AS event_actor_group,
        LENGTH(TRIM(event_event_type)) AS event_event_type_len
    FROM with_children
)
SELECT
    event_type,
    event_at_ym,
    n_actor_in_system_agent,
    n_rows,
    event_id,
    event_actor_group,
    event_event_type_len
FROM scored
ORDER BY n_actor_in_system_agent DESC, event_id DESC
