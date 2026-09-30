WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_actor_label,
        s.actor_group,
        t.tags AS ticket_tags
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE SIZE(t.tags) >= 1
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_details_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'assigned'
        )
      AND ARRAY_CONTAINS(s.ticket_tags, 'outage')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_actor_group_by_event_at AS event_at_quartile,
        NTILE(3) OVER w_actor_group_by_event_at AS event_at_tercile,
        LAST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at), ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM without_matches
    WINDOW w_actor_group_by_event_at AS (PARTITION BY actor_group ORDER BY event_at DESC, id)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_type_actor_label,
    actor_group,
    ticket_tags,
    event_at_quartile,
    event_at_tercile,
    last_event_at
FROM with_trends
ORDER BY event_type NULLS LAST, event_at, id
LIMIT 20
