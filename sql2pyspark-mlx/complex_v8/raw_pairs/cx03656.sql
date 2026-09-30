WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-01-31'
      AND actor IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_dense_rank,
        DENSE_RANK() OVER w_event_type_actor_by_event_at_3 AS event_at_dense_rank_2,
        LEAD(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at, id) AS next2_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at), 3) AS event_at_percent_rank
    FROM base_events
    WINDOW w_event_type_actor_by_event_at_3 AS (PARTITION BY event_type, actor ORDER BY event_at DESC)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(event_at) AS event_at_date,
        COALESCE(event_at_dense_rank, 0) AS event_at_dense_rank_filled
    FROM with_trends
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(event_at_percent_rank) OVER (PARTITION BY event_at_dense_rank_filled, event_at_dense_rank ORDER BY second_event_at ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_at_percent_rank_avg_2,
        ROUND(CUME_DIST() OVER w_event_at_dense_rank_filled_e_2, 3) AS second_event_at_cume_dist,
        LEAD(second_event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS next_second_event_at
    FROM scored
    WINDOW w_event_at_dense_rank_filled_e_2 AS (PARTITION BY event_at_dense_rank_filled, event_at_dense_rank ORDER BY second_event_at ASC NULLS LAST, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE event_at_dense_rank_2 <= 3
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        COALESCE(event_at_dense_rank, 0) AS event_at_dense_rank_filled_2,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM top_rows
    WHERE event_at >= TIMESTAMP '2025-01-31 10:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM scored_2 s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    second_event_at,
    event_at_dense_rank,
    event_at_dense_rank_2,
    event_at_date,
    event_at_dense_rank_filled,
    event_at_percent_rank_avg_2,
    next_second_event_at,
    days_since_event_at,
    actor_prefix,
    event_count
FROM profiled
ORDER BY actor_prefix DESC NULLS FIRST
