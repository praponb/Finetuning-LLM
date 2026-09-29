WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 08:00:00'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_len,
        s.event_type_ref,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM prepared s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type <> 'escalated'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type_ref,
        event_id,
        event_event_at,
        CASE WHEN event_type_len >= 11 THEN 'A' WHEN event_type_len >= 7 THEN 'B' ELSE 'C' END AS event_type_len_tier,
        COALESCE(event_type_len, 0) AS event_type_len_filled
    FROM exploded_detail_events
    WHERE ticket_id IS NOT NULL
)
SELECT
    event_type_len_tier,
    event_type_len_filled,
    event_id,
    GROUPING(event_type_len_tier) AS is_total_event_type_len_tier,
    MAX(event_event_at) AS last_event_event_at,
    MIN(event_at) AS first_event_at
FROM labeled
GROUP BY GROUPING SETS ((event_type_len_tier, event_type_len_filled, event_id), (event_type_len_tier), (event_type_len_filled), ())
ORDER BY event_type_len_tier NULLS LAST, event_type_len_filled NULLS LAST, event_id NULLS LAST
