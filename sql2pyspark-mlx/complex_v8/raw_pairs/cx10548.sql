WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor = 'system')
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM prepared_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
summary AS (
    -- Summarise per group
    SELECT
        ticket_id,
        event_actor,
        TO_DATE(event_event_at) AS event_event_at_day,
        MAX(event_event_at) AS last_event_event_at,
        SUM(CASE WHEN event_type NOT IN ('reopened', 'escalated') THEN 1 ELSE 0 END) AS n_event_type_not_in_reopened_e,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(*) AS n_rows
    FROM exploded_detail_events
    WHERE ticket_id <= 52
    GROUP BY ticket_id, event_actor, TO_DATE(event_event_at)
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    ticket_id,
    event_actor,
    last_event_event_at,
    n_event_type_not_in_reopened_e,
    event_type_list,
    n_rows
FROM summary
ORDER BY event_actor NULLS LAST, last_event_event_at DESC
