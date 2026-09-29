WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM valid s
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM summarized_rows s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
),
event_stats_3 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE event_type = 'assigned') AS event_event_type_count,
        COUNT(CASE WHEN event_type = 'reopened' THEN 1 END) AS event_event_type_count_2,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows_2 AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        COALESCE(t.event_event_type_count_2, 0) AS event_event_type_count_2,
        t.first_event_at
    FROM with_stats s
    LEFT JOIN event_stats_3 t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type_actor_label,
    event_at_dayofweek,
    event_count,
    event_event_type_count,
    event_event_type_count_2,
    first_event_at
FROM summarized_rows_2
ORDER BY event_count, id
LIMIT 20
