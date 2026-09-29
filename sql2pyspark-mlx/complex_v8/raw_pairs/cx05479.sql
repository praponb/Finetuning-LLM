WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE (TO_DATE(event_at) < DATE '2025-01-31' OR event_type IS NOT NULL)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_at_month) AS days_since_event_at_month,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        LENGTH(TRIM(event_type_or_unknown)) AS event_type_or_unknown_len
    FROM prepared_events
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    event_type_or_unknown,
    MAX(event_at) AS last_event_at,
    MIN(event_at_month) AS first_event_at_month
FROM derived
GROUP BY GROUPING SETS ((event_type, event_type_or_unknown), (event_type), ())
