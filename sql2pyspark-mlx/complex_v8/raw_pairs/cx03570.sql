WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor IN ('agent', 'system') OR ticket_id >= 14)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at) OVER w_event_type_ticket_id_by_even AS next_event_at,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        LAG(event_at, 3) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS prev3_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS rn,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no,
        LEAD(event_at) OVER (ORDER BY event_at DESC, id) AS next_event_at_2,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        NTILE(10) OVER (ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC), 3) AS event_at_percent_rank
    FROM cleaned_events
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE event_at_rank <= 2
      AND event_at >= TIMESTAMP '2025-01-19 11:00:00'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        actor,
        DATE_TRUNC('MONTH', next_event_at_2) AS next_event_at_2_month,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        COUNT(*) AS row_count,
        ROUND(SUM(seq_no) / NULLIF(SUM(event_at_rank), 0), 3) AS seq_no_to_event_at_rank_ratio
    FROM first_rows
    GROUP BY 1, 2, 3
    HAVING AVG(rn) > 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(row_count) OVER (ORDER BY row_count, actor, next_event_at_2_month, event_at_ym ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_3,
        COUNT(*) OVER (PARTITION BY actor, row_count) AS rows_in_actor_row_count
    FROM rollup_base
)
-- Final projection
SELECT
    actor,
    event_at_ym,
    row_count,
    seq_no_to_event_at_rank_ratio,
    row_count_avg_3,
    rows_in_actor_row_count,
    DATE_FORMAT(next_event_at_2_month, 'yyyy-MM-dd HH:00') AS next_event_at_2_month_label
FROM metrics
ORDER BY row_count_avg_3, next_event_at_2_month_label NULLS LAST
