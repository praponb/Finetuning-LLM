WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor <> 'customer')
),
grouped AS (
    -- Group-level totals
    SELECT
        event_type,
        event_type_or_unknown,
        ticket_id,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS n_rows
    FROM base
    GROUP BY 1, 2, 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, event_type_or_unknown, event_type, n_rows
    FROM grouped
) src
PIVOT (
    SUM(n_rows) AS amount, MAX(n_rows) AS max_v
    FOR event_type IN ('resolved', 'escalated')
)
ORDER BY ticket_id, event_type_or_unknown
