WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 09:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at) OVER w_event_type_by_event_at AS prev_event_at,
        ROW_NUMBER() OVER w_event_type_by_event_at AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS prev3_event_at
    FROM base
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.requester_id BETWEEN 7 AND 19
              AND HOUR(t.opened_at) BETWEEN 7 AND 12
        )
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_3,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM filtered s
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER (ORDER BY prev_event_at DESC, id))) / 60, 1) AS minutes_since_prev_second_event_at,
        NTILE(10) OVER w_all_by_prev_event_at AS prev_event_at_decile,
        LAG(seq_no, 2, 0) OVER (ORDER BY prev_event_at DESC, id) AS prev2_seq_no
    FROM with_stats
    WINDOW w_all_by_prev_event_at AS (ORDER BY prev_event_at DESC, id)
),
with_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.priority AS ticket_priority
    FROM sequenced_2 s
    JOIN tickets t ON t.id = s.ticket_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        last_event_at,
        prev_event_at,
        seq_no,
        prev3_event_at,
        last_event_at_2,
        first_event_at,
        minutes_since_prev_second_event_at,
        prev_event_at_decile,
        ticket_priority,
        LENGTH(TRIM(actor_event_type_label)) AS actor_event_type_label_len,
        FLOOR(seq_no / 2) * 2 AS seq_no_bucket,
        ABS(event_at_cume_dist - 0.6) AS event_at_cume_dist_distance
    FROM with_details_ticket
    WHERE second_event_at < TIMESTAMP '2025-02-24 13:00:00'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, minutes_since_prev_second_event_at, event_at_cume_dist_distance
    FROM scored
) src
UNPIVOT (
    value FOR measure IN (minutes_since_prev_second_event_at, event_at_cume_dist_distance)
) unp
