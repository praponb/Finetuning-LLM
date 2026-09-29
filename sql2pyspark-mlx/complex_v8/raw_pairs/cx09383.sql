WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty > 189
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        operator,
        session_no,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY operator, session_no
),
labeled AS (
    -- Add calculated columns
    SELECT
        operator,
        session_no,
        session_start,
        session_end,
        n_events,
        duration_min,
        session_start + INTERVAL 1 DAYS AS session_start_plus_1d,
        session_start + INTERVAL 15 MINUTES AS session_start_plus_15m,
        GREATEST(duration_min, session_unit_price) AS greatest_duration_min_session_unit_price
    FROM per_session
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(operator)) AS operator_clean,
        DATE_FORMAT(session_start, 'yyyy-MM-dd HH:00') AS session_start_label,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM labeled
    WHERE session_end >= TIMESTAMP '2025-01-09 09:00:00'
)
-- Native SQL UNPIVOT (wide to long)
SELECT operator, session_no, metric_name, reading
FROM (
    SELECT operator, session_no, greatest_duration_min_session_unit_price, duration_min
    FROM enriched
) src
UNPIVOT (
    reading FOR metric_name IN (greatest_duration_min_session_unit_price, duration_min)
) unp
