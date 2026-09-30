WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id >= 49 OR event_type NOT IN ('replied', 'reopened'))
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM valid
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    event_at_label,
    LENGTH(TRIM(event_type_prefix)) AS event_type_prefix_len
FROM labeled
WHERE event_type_prefix IS NOT NULL
ORDER BY event_type_prefix_len
