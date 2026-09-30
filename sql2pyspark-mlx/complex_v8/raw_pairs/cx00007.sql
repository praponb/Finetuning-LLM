WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM recent_events s
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', event_type, actor_group) AS event_type_actor_group_label,
        ROUND((UNIX_TIMESTAMP(first_event_at) - UNIX_TIMESTAMP(event_at_day)) / 60, 1) AS minutes_event_at_day_to_first_event_at,
        CASE WHEN actor_group IN ('other', 'group_a') THEN 'group_a' WHEN actor_group = 'group_b' THEN 'group_b' ELSE 'other' END AS actor_group_group
    FROM with_counts
),
combined_details AS (
    -- Join tickets
    SELECT s.*, t.channel AS ticket_channel
    FROM tagged s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_event_at, 2) OVER (PARTITION BY ticket_channel ORDER BY minutes_event_at_day_to_first_event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_event_at,
        RANK() OVER (PARTITION BY ticket_channel ORDER BY minutes_event_at_day_to_first_event_at NULLS LAST) AS minutes_event_at_day_to_first_event_at_rank
    FROM combined_details
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_day,
    actor_group,
    first_event_at,
    last_event_at,
    event_type_actor_group_label,
    minutes_event_at_day_to_first_event_at,
    second_first_event_at,
    minutes_event_at_day_to_first_event_at_rank,
    DATE_TRUNC('MONTH', event_at) AS event_at_month
FROM with_windows
ORDER BY event_type_actor_group_label DESC, ticket_id DESC, id
LIMIT 20
