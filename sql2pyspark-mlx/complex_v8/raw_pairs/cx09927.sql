WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id AND t.csat < 5
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at, 3) OVER w_ticket_id_by_event_at AS next3_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS seq_no,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS prev_event_at,
        NTILE(10) OVER w_ticket_id_by_event_at AS event_at_decile,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC) AS event_at_dense_rank
    FROM joined_ticket
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
),
deduped AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_tags,
        ticket_priority,
        ticket_closed_at,
        next3_event_at,
        seq_no,
        last_event_at,
        prev_event_at,
        event_at_decile,
        event_at_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY next3_event_at DESC, id DESC) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(prev_event_at, 'yyyy-MM-dd HH:00') AS prev_event_at_label,
        COALESCE(event_at_decile, 0) AS event_at_decile_filled,
        ROUND(event_at_decile / NULLIF(event_at_dense_rank, 0), 3) AS event_at_decile_per_event_at_dense_rank
    FROM deduped
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_priority,
        ticket_closed_at,
        next3_event_at,
        last_event_at,
        prev_event_at,
        event_at_decile,
        event_at_dense_rank,
        event_at_decile_filled,
        DATE_TRUNC('HOUR', next3_event_at) AS next3_event_at_hour
    FROM enriched
    WHERE HOUR(last_event_at) BETWEEN 10 AND 16
)
SELECT
    event_at_dense_rank,
    event_at_decile_filled,
    COUNT(*) AS row_count
FROM tagged
GROUP BY GROUPING SETS ((event_at_dense_rank, event_at_decile_filled), (event_at_dense_rank))
