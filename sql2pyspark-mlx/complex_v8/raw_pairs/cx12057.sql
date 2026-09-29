WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'reopened') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE (actor NOT IN ('agent', 'system') OR HOUR(event_at) BETWEEN 9 AND 13)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at
    FROM prepared_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor <> 'customer'
        )
      AND s.event_type_group = 'group_a'
)
SELECT
    ticket_id,
    event_type_group,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(*) AS cnt
FROM with_activity
GROUP BY GROUPING SETS ((ticket_id, event_type_group), (ticket_id), (event_type_group), ())
ORDER BY ticket_id NULLS LAST, event_type_group NULLS LAST
