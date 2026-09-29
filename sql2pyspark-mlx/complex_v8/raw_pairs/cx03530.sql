WITH valid AS (
    -- Keep valid rows and derive helper columns
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
    WHERE warehouse_id IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.prev_moved_at,
        s.new_session_flag,
        s.session_no,
        sk.labels AS sku_labels
    FROM sessionized s
    JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        qty,
        operator,
        session_no,
        FIRST_VALUE(unit_price) OVER (PARTITION BY move_type, session_no ORDER BY prev_moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price,
        ROW_NUMBER() OVER (PARTITION BY move_type, session_no ORDER BY prev_moved_at ASC, id) AS prev_moved_at_seq,
        NTILE(3) OVER (PARTITION BY warehouse_id, move_type ORDER BY prev_moved_at NULLS LAST, id) AS prev_moved_at_tercile
    FROM joined_sku
)
SELECT DISTINCT prev_moved_at_tercile
FROM sequenced
WHERE operator NOT IN ('op_dew', 'op_eve')
