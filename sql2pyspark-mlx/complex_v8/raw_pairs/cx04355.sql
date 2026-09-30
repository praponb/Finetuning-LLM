WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'customer')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM base s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
stats AS (
    -- Summarise per group
    SELECT
        ticket_id,
        MAX(event_count) AS max_event_count,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('created', 'escalated', 'assigned') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_created_escala,
        ROUND(STDDEV(event_count), 2) AS stddev_event_count,
        MAX(event_at) AS last_event_at,
        SUM(event_count) FILTER (WHERE actor IN ('system', 'customer')) AS event_count_actor_in_system_customer,
        SUM(event_count) AS total_event_count
    FROM summarized_rows
    GROUP BY ticket_id
    HAVING SUM(event_count) > 3
)
-- Final projection
SELECT
    ticket_id,
    max_event_count,
    stddev_event_count,
    event_count_actor_in_system_customer,
    total_event_count
FROM stats
ORDER BY event_count_actor_in_system_customer, total_event_count DESC, ticket_id
LIMIT 10
