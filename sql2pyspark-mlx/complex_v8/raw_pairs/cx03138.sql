WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_type,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        MIN(event_at) AS first_event_at,
        MIN(hours_since_open) AS min_hours_since_open,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open
    FROM covered
    GROUP BY event_type, DATE_TRUNC('MONTH', event_at)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(first_event_at, 2) OVER (PARTITION BY event_type, MONTH(event_at_month) ORDER BY median_hours_since_open DESC, event_type, event_at_month) AS prev2_first_event_at,
        MAX(min_hours_since_open) OVER (PARTITION BY event_type, MONTH(event_at_month) ORDER BY median_hours_since_open DESC, event_type, event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_min_hours_since_open_so_far,
        ROUND(min_hours_since_open - AVG(min_hours_since_open) OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at)), 2) AS min_hours_since_open_vs_first_event_at_month_avg,
        COUNT(*) OVER w_first_event_at_month AS rows_in_first_event_at_month,
        DENSE_RANK() OVER (PARTITION BY event_type, TO_DATE(event_at_month) ORDER BY first_event_at) AS first_event_at_dense_rank,
        SUM(median_hours_since_open) OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at)) AS first_event_at_month_sum_median_hours_since_open
    FROM rollup_base
    WINDOW w_first_event_at_month AS (PARTITION BY DATE_TRUNC('MONTH', first_event_at))
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE rows_in_first_event_at_month < (SELECT MAX(rows_in_first_event_at_month) FROM ranked)
)
SELECT
    event_type,
    event_at_month,
    median_hours_since_open,
    prev2_first_event_at,
    max_min_hours_since_open_so_far,
    first_event_at_dense_rank,
    first_event_at_month_sum_median_hours_since_open,
    INITCAP(TRIM(event_type)) AS event_type_clean
FROM without_matches
ORDER BY event_type
