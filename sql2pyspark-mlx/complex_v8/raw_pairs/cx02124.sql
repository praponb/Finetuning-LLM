WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_type NOT IN ('escalated', 'created')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_type_actor_label,
        event_at_week,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at_week DESC NULLS FIRST), 3) AS event_at_week_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_week, 3) AS event_at_week_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at_week DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at_week) OVER (ORDER BY event_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_week
    FROM filtered
    WINDOW w_all_by_event_at_week AS (ORDER BY event_at_week, id)
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'system'
        )
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    actor_event_type_label,
    event_type_actor_label,
    event_at_week_cume_dist,
    event_at_week_percent_rank,
    second_event_at,
    DATEDIFF(DATE '2025-06-30', second_event_at) AS days_since_second_event_at,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
FROM without_matches
ORDER BY days_since_second_event_at NULLS LAST, id
LIMIT 20
