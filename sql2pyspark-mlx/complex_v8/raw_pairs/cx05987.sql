WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix_2,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix_3
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 11:00:00'
      AND event_type <> 'reopened'
),
latest AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        actor_prefix_2,
        actor_prefix_3
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM prepared_events
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER w_actor_by_event_at AS event_at_tercile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER w_ticket_id_actor_by_event_at_2, 3) AS event_at_cume_dist,
        LEAD(event_at, 3) OVER w_ticket_id_actor_by_event_at_3 AS next3_event_at
    FROM latest
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id), w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC), w_ticket_id_actor_by_event_at_3 AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id)
),
event_stats AS (
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
    FROM with_trends s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2,
        (SELECT COUNT(*) FILTER (WHERE t.event_type = 'reopened') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count
    FROM with_stats s
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(second_event_at)) / 3600, 1) AS hours_second_event_at_to_event_at,
        ROUND(event_at_cume_dist * 0.9, 2) AS event_at_cume_dist_adj
    FROM summarized_rows
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_3,
        (SELECT COUNT(*) FILTER (WHERE t.event_type = 'replied') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count_2
    FROM annotated s
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_prefix,
    actor_prefix_2,
    actor_prefix_3,
    event_at_tercile,
    second_event_at,
    event_at_cume_dist,
    next3_event_at,
    event_count,
    event_count_2,
    event_event_type_count,
    event_count_3,
    event_event_type_count_2
FROM with_counts
WHERE event_count_2 >= 1
ORDER BY actor_prefix DESC
