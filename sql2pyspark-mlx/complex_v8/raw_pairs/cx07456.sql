WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
),
with_prev AS (
    -- Previous moved_at per operator
    SELECT *, LAG(moved_at) OVER (PARTITION BY operator ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR (UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60 > 60 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY operator ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
annotated AS (
    -- Label and score rows
    SELECT *, unit_price_adj + qty AS unit_price_adj_plus_qty
    FROM sessionized
    WHERE unit_price_adj <= 487.9
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price_adj,
    prev_moved_at,
    new_session_flag,
    unit_price_adj_plus_qty,
    SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
FROM annotated
WHERE unit_price_adj <= 562.8
ORDER BY new_session_flag DESC NULLS FIRST
