WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label_2
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'escalated')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_label,
        days_since_event_at,
        event_at_label_2,
        SIZE(COLLECT_SET(event_type) OVER w_actor_ticket_id) AS distinct_event_type_in_actor_ticket_id,
        LAST_VALUE(event_at) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER w_event_at_month_by_event_at AS seq_no
    FROM base_events
    WINDOW w_actor_ticket_id AS (PARTITION BY actor, ticket_id), w_event_at_month_by_event_at AS (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id)
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IS NOT NULL
        )
      AND s.ticket_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_at_label_2,
    distinct_event_type_in_actor_ticket_id,
    last_event_at,
    seq_no,
    CONCAT_WS(' / ', event_at_label_2, event_type) AS event_at_label_2_event_type_label
FROM screened
ORDER BY event_at_label_2 DESC
