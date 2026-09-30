WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE actor NOT IN ('agent', 'system')) AS event_actor_count,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.first_event_at
    FROM base_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_actor_count,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM profiled s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.opened_at < TIMESTAMP '2025-02-18 07:00:00'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(event_actor_count) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at) ORDER BY event_actor_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_actor_count,
        ROUND(AVG(event_actor_count) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_opened_at) ORDER BY event_actor_count, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_actor_count_avg_3
    FROM with_details
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY ticket_id ORDER BY running_event_actor_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        COUNT(*) OVER w_actor AS rows_in_actor,
        COUNT(*) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY ticket_id ORDER BY running_event_actor_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        SUM(event_actor_count) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_actor_count_2
    FROM ranked
    WINDOW w_actor AS (PARTITION BY actor)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        event_actor_count,
        ticket_closed_at,
        ticket_opened_at,
        running_event_actor_count,
        rows_in_actor,
        running_count,
        last_ticket_closed_at,
        running_event_actor_count_2,
        LAG(last_ticket_closed_at, 3) OVER (PARTITION BY event_actor_count ORDER BY running_event_actor_count NULLS LAST, id) AS prev3_last_ticket_closed_at,
        ROUND(AVG(rows_in_actor) OVER (PARTITION BY event_type, actor), 2) AS event_type_actor_avg_rows_in_actor
    FROM ranked_2
)
-- Final projection
SELECT DISTINCT rows_in_actor, id, actor
FROM windowed
ORDER BY id
