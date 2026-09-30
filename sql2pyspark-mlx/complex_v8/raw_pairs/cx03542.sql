WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'assigned') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel NOT IN ('chat', 'web')
              AND t.requester_id BETWEEN 3 AND 24
        )
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_group,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM with_activity s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_group,
        event_id,
        event_event_at,
        ROUND((UNIX_TIMESTAMP(event_event_at) - UNIX_TIMESTAMP(LAG(event_event_at) OVER (ORDER BY event_event_at DESC NULLS FIRST, id, event_id))) / 60, 1) AS minutes_since_prev_event_event_at,
        NTH_VALUE(event_event_at, 2) OVER (ORDER BY event_event_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_event_at
    FROM line_level_events
)
-- Final projection
SELECT
    id,
    event_type,
    event_type_group,
    event_id,
    event_event_at,
    minutes_since_prev_event_event_at,
    second_event_event_at
FROM metrics
WHERE event_type = 'assigned'
ORDER BY id DESC
