WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
),
stats AS (
    -- Summarise per group
    SELECT
        event_type_or_unknown,
        ticket_id,
        ROUND(100 * COUNT(CASE WHEN actor IN ('system', 'customer') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_system_customer,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS cnt
    FROM recent
    WHERE event_type_or_unknown <> 'created'
    GROUP BY event_type_or_unknown, ticket_id
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY event_type_or_unknown) AS rows_in_event_type_or_unknown,
        FIRST_VALUE(first_event_at) IGNORE NULLS OVER (ORDER BY last_event_at, event_type_or_unknown, ticket_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_event_at
    FROM stats
)
-- Final projection
SELECT
    event_type_or_unknown,
    ticket_id,
    pct_actor_in_system_customer,
    last_event_at,
    rows_in_event_type_or_unknown,
    first_known_first_event_at
FROM with_windows
