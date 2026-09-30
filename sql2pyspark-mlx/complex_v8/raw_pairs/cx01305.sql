WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_type = 'assigned' OR ticket_id IS NOT NULL)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM base_events s
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.opened_at >= TIMESTAMP '2025-02-06 11:00:00'
              AND t.csat > 2
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        last_event_at,
        SUM(event_count) OVER (PARTITION BY ticket_id) AS ticket_id_sum_event_count,
        MIN(event_count) OVER (PARTITION BY ticket_id) AS ticket_id_min_event_count,
        ROUND(event_count - AVG(event_count) OVER (PARTITION BY ticket_id), 2) AS event_count_vs_ticket_id_avg
    FROM with_activity
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(actor)) AS actor_len,
        DATE_FORMAT(last_event_at, 'MMM yyyy') AS last_event_at_label
    FROM ranked
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(last_event_at) OVER (PARTITION BY actor ORDER BY last_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_event_at,
        COUNT(*) OVER (PARTITION BY actor_len) AS rows_in_actor_len,
        MAX(ticket_id_min_event_count) OVER (PARTITION BY actor_len) AS actor_len_max_ticket_id_min_event_count,
        MAX(event_count_vs_ticket_id_avg) OVER (PARTITION BY actor ORDER BY last_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_count_vs_ticket_id_avg_so_far,
        MAX(ticket_id_sum_event_count) OVER (PARTITION BY actor_len, DATE_TRUNC('MONTH', event_at) ORDER BY ticket_id_min_event_count NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_id_sum_event_count_so_far
    FROM enriched
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        actor,
        last_event_at,
        ticket_id_sum_event_count,
        ticket_id_min_event_count,
        event_count_vs_ticket_id_avg,
        actor_len,
        last_event_at_label,
        first_last_event_at,
        rows_in_actor_len,
        actor_len_max_ticket_id_min_event_count,
        max_event_count_vs_ticket_id_avg_so_far,
        ROUND(rows_in_actor_len / NULLIF(actor_len, 0), 3) AS rows_in_actor_len_per_actor_len,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM with_windows
    WHERE actor_len_max_ticket_id_min_event_count < 720
)
SELECT
    id,
    ticket_id_sum_event_count,
    ticket_id_min_event_count,
    actor_len,
    first_last_event_at,
    rows_in_actor_len,
    max_event_count_vs_ticket_id_avg_so_far,
    rows_in_actor_len_per_actor_len,
    event_at_week,
    MONTH(last_event_at) AS last_event_at_month,
    DATE_TRUNC('HOUR', last_event_at) AS last_event_at_hour
FROM enriched_2
