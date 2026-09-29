WITH prepared_moves AS (
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
    WHERE warehouse_id >= 2
      AND HOUR(moved_at) BETWEEN 8 AND 14
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        qty - LAG(qty) OVER (ORDER BY moved_at, id) AS qty_change,
        ROW_NUMBER() OVER w_all_by_moved_at AS moved_at_seq,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id, 0), 4) AS qty_share_of_warehouse_id,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank
    FROM prepared_moves
    WINDOW w_all_by_moved_at AS (ORDER BY moved_at, id), w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    moved_at_seq,
    sku_id,
    COUNT(*) AS cnt
FROM windowed
GROUP BY GROUPING SETS ((moved_at_seq, sku_id), (moved_at_seq), ())
