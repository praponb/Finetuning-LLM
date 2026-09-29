WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE actor = 'system'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        DATE_TRUNC('HOUR', event_at_plus_7d) AS event_at_plus_7d_hour,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix_2
    FROM prepared
    WHERE ticket_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_prefix,
        event_at_plus_7d_hour,
        event_type_prefix_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM tagged
    ) ranked
    WHERE row_num = 1
)
SELECT
    event_type,
    ticket_id,
    ROUND(100 * COUNT(CASE WHEN event_type = 'assigned' THEN 1 END) / COUNT(*), 1) AS pct_event_type_assigned,
    COUNT(*) AS n_rows
FROM deduped
GROUP BY GROUPING SETS ((event_type, ticket_id), (event_type), ())
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST
