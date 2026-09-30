WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        CASE WHEN event_type IN ('escalated', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type, event_type_group ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type_group ORDER BY event_at) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_actor_event_type_by_event_at, 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (PARTITION BY ticket_id, event_type_group ORDER BY event_at, id) AS next_event_at
    FROM cleaned
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at_2, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at_2,
        COALESCE(t.event_count, 0) AS event_count
    FROM sequenced s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT id, measure, value
FROM (
    SELECT id, event_at_dense_rank, event_count
    FROM profiled
) src
UNPIVOT (
    value FOR measure IN (event_at_dense_rank, event_count)
) unp
