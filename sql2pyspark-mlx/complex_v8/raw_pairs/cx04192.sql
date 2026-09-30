WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE ticket_id >= 59
      AND actor NOT IN ('system', 'agent')
),
latest AS (
    -- Keep the earliest row per actor, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_clean,
        event_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS rn
        FROM valid_events
    ) ranked
    WHERE rn = 1
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM latest s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at_2
    FROM exploded_detail s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at_3,
        COUNT(CASE WHEN actor IN ('agent', 'customer') THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at_3,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM summarized_rows s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
)
SELECT
    event_id,
    ticket_id,
    event_count,
    GROUPING(event_id) AS is_total_event_id,
    COUNT(*) AS n_rows
FROM with_counts
GROUP BY GROUPING SETS ((event_id, ticket_id, event_count), (event_id), (ticket_id))
ORDER BY event_id NULLS LAST, ticket_id NULLS LAST, event_count NULLS LAST
