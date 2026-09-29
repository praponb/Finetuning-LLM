WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        UPPER(TRIM(move_type)) AS move_type_clean
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        SUM(qty) OVER (PARTITION BY sku_id, TO_DATE(moved_at) ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty,
        FIRST_VALUE(qty) OVER (PARTITION BY move_type ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        LAST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        SUM(qty) OVER (PARTITION BY move_type, TO_DATE(moved_at)) AS move_type_moved_at_to_date_sum_qty,
        MIN(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER w_warehouse_id, 0), 4) AS qty_share_of_warehouse_id,
        ROUND(qty - AVG(qty) OVER (PARTITION BY move_type, TO_DATE(moved_at)), 2) AS qty_vs_move_type_moved_at_to_date_avg,
        RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at ASC, id) AS moved_at_rank,
        COUNT(*) OVER (PARTITION BY move_type, TO_DATE(moved_at)) AS rows_in_move_type_moved_at_to_date,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, move_type ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior
    FROM valid_moves
    WINDOW w_warehouse_id AS (PARTITION BY warehouse_id)
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    running_qty,
    last_known_qty,
    move_type_moved_at_to_date_sum_qty,
    warehouse_id_min_unit_price,
    qty_share_of_warehouse_id,
    qty_vs_move_type_moved_at_to_date_avg,
    moved_at_rank,
    rows_in_move_type_moved_at_to_date,
    unit_price_avg_1_prior,
    DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
    CONCAT(move_type, '#', CAST(running_qty AS STRING)) AS move_type_ref
FROM metrics
ORDER BY id NULLS LAST, warehouse_id DESC
LIMIT 10
