WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM stock_moves
    WHERE qty >= 152
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        warehouse_id,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, session_no
),
enriched AS (
    -- Label and score rows
    SELECT
        warehouse_id,
        session_no,
        session_start,
        session_end,
        event_count,
        session_unit_price,
        duration_min,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket_2,
        COALESCE(event_count, 0) AS event_count_filled
    FROM per_session
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        w.capacity AS warehouse_capacity,
        w.opened_on AS warehouse_opened_on,
        w.wh_code AS warehouse_wh_code
    FROM enriched s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    warehouse_id,
    session_no,
    session_start,
    session_end,
    event_count,
    session_unit_price,
    duration_min,
    duration_min_bucket_2,
    event_count_filled,
    warehouse_capacity,
    DATE_FORMAT(session_start, 'MMM yyyy') AS session_start_label
FROM joined
ORDER BY session_start DESC NULLS FIRST, duration_min NULLS LAST
