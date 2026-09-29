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
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM stock_moves
    WHERE sku_id >= 3
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined AS (
    -- Attach warehouse attributes
    SELECT s.*, w.opened_on AS warehouse_opened_on, sk.labels AS sku_labels
    FROM sessionized s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(prev_moved_at)) AS rows_in_prev_moved_at_year,
        FIRST_VALUE(starts_session) IGNORE NULLS OVER (PARTITION BY move_type, starts_session ORDER BY qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_session,
        ROUND(starts_session / NULLIF(SUM(starts_session) OVER (PARTITION BY YEAR(prev_moved_at)), 0), 4) AS starts_session_share_of_prev_moved_at_year
    FROM joined
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    qty,
    moved_at,
    operator,
    unit_price,
    moved_at_label,
    starts_session,
    warehouse_opened_on,
    sku_labels,
    rows_in_prev_moved_at_year,
    first_known_starts_session,
    starts_session_share_of_prev_moved_at_year,
    CONCAT_WS(' / ', move_type, moved_at_label) AS move_type_moved_at_label_label,
    DATE_FORMAT(prev_moved_at, 'MMM yyyy') AS prev_moved_at_label
FROM with_trends
WHERE HOUR(prev_moved_at) BETWEEN 9 AND 12
ORDER BY moved_at DESC, id
LIMIT 5
