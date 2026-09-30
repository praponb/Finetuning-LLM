WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE (actor IN ('customer', 'agent') OR event_type = 'resolved')
),
combined_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.opened_at AS ticket_opened_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE HOUR(t.opened_at) BETWEEN 7 AND 12
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.requester_id > 5
        )
)
SELECT *
FROM (
    SELECT event_type, ticket_opened_at, actor_group, event_at_hour
    FROM with_activity
) src
PIVOT (
    COUNT(event_at_hour)
    FOR actor_group IN ('group_a', 'group_b')
)
ORDER BY event_type, ticket_opened_at
