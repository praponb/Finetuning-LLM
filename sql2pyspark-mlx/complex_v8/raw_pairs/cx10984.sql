WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-03-02 12:00:00'
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_no
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(session_start, 'yyyy-MM') AS session_start_ym,
        event_count,
        MAX(duration_min) AS max_duration_min,
        SUM(event_count) AS total_event_count,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(event_count, 0.5) AS median_event_count
    FROM session_summary
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        session_start_ym,
        event_count,
        max_duration_min,
        total_event_count,
        row_count,
        ROUND(PERCENT_RANK() OVER w_all_by_row_count, 3) AS row_count_percent_rank,
        RANK() OVER (ORDER BY row_count) AS row_count_rank,
        COUNT(*) OVER (PARTITION BY row_count) AS rows_in_row_count,
        ROUND(AVG(max_duration_min) OVER (PARTITION BY row_count), 2) AS row_count_avg_max_duration_min,
        MAX(row_count) OVER (PARTITION BY row_count) AS row_count_max_row_count
    FROM stats
    WINDOW w_all_by_row_count AS (ORDER BY row_count, session_start_ym, event_count)
)
SELECT session_start_ym, event_count, metric_name, value
FROM (
    SELECT session_start_ym, event_count, row_count_max_row_count, rows_in_row_count, row_count
    FROM ranked
) src
UNPIVOT (
    value FOR metric_name IN (row_count_max_row_count, rows_in_row_count, row_count)
) unp
