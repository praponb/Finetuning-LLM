WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor = 'system'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM scoped_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor_len,
        days_since_event_at,
        event_type_actor_label,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER w_event_type_event_at_to_date, 0), 4) AS actor_len_share_of_event_type_event_at_to_date,
        DENSE_RANK() OVER (ORDER BY last_event_at DESC, id) AS last_event_at_dense_rank,
        NTH_VALUE(days_since_event_at, 2) OVER (ORDER BY last_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_event_at,
        LAG(event_at, 2) OVER w_all_by_last_event_at AS prev2_event_at
    FROM summarized_rows
    WINDOW w_event_type_event_at_to_date AS (PARTITION BY event_type, TO_DATE(event_at)), w_all_by_last_event_at AS (ORDER BY last_event_at DESC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY days_since_event_at DESC), 3) AS days_since_event_at_cume_dist,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY MONTH(prev2_event_at), ticket_id), 2) AS days_since_event_at_vs_prev2_event_at_month_tick,
        RANK() OVER (ORDER BY days_since_event_at DESC, id) AS days_since_event_at_rank,
        last_event_at_dense_rank - LAG(last_event_at_dense_rank) OVER (ORDER BY days_since_event_at DESC, id) AS last_event_at_dense_rank_change
    FROM metrics
)
-- Subtotals with GROUPING SETS
SELECT
    last_event_at_dense_rank,
    actor_len,
    MAX(prev2_event_at) AS last_prev2_event_at,
    MIN(prev2_event_at) AS first_prev2_event_at
FROM with_trends
GROUP BY GROUPING SETS ((last_event_at_dense_rank, actor_len), (last_event_at_dense_rank), (actor_len), ())
