WITH valid_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE qty < 370
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        sk.sku_code,
        sk.category AS sku_category,
        sk.is_active AS sku_is_active,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id AND UPPER(TRIM(sk.sku_code)) LIKE 'E%'
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE sk.is_active = TRUE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY warehouse_opened_on DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        NTH_VALUE(warehouse_capacity, 2) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_capacity,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY moved_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS warehouse_capacity_avg_3_prior,
        LEAD(warehouse_opened_on) OVER (PARTITION BY move_type ORDER BY warehouse_opened_on DESC, id) AS next_warehouse_opened_on,
        COUNT(*) OVER (PARTITION BY move_type, warehouse_id) AS rows_in_move_type_warehouse_id,
        RANK() OVER w_move_type_by_moved_at AS moved_at_rank,
        ROUND(AVG(qty) OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_1_prior,
        LAST_VALUE(qty) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER w_move_type_warehouse_opened_o, 2) AS warehouse_capacity_vs_move_type_warehouse_opened,
        LAST_VALUE(qty) IGNORE NULLS OVER (ORDER BY warehouse_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_qty,
        COUNT(*) OVER (PARTITION BY move_type, sku_is_active) AS rows_in_move_type_sku_is_active,
        ROUND(AVG(warehouse_capacity) OVER (PARTITION BY move_type ORDER BY warehouse_opened_on DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS warehouse_capacity_avg_4,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY move_type, sku_is_active ORDER BY warehouse_opened_on ASC, id))) / 60, 1) AS minutes_since_prev_moved_at_2,
        MIN(warehouse_capacity) OVER (PARTITION BY sku_is_active, warehouse_id ORDER BY moved_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_warehouse_capacity_so_far,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER w_move_type_warehouse_id, 0), 4) AS warehouse_capacity_share_of_move_type_warehouse_
    FROM with_parent
    WINDOW w_move_type_by_moved_at AS (PARTITION BY move_type ORDER BY moved_at, id), w_move_type_warehouse_opened_o AS (PARTITION BY move_type, MONTH(warehouse_opened_on)), w_sku_is_active_warehouse_id_b AS (PARTITION BY sku_is_active, warehouse_id ORDER BY warehouse_opened_on DESC, id), w_move_type_warehouse_id AS (PARTITION BY move_type, warehouse_id)
),
annotated AS (
    -- Derive helper columns
    SELECT *, CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag
    FROM with_trends
    WHERE move_type IN ('ADJUST', 'TRANSFER')
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    sku_code,
    sku_is_active,
    warehouse_opened_on,
    warehouse_capacity,
    minutes_since_prev_moved_at,
    second_warehouse_capacity,
    warehouse_capacity_avg_3_prior,
    rows_in_move_type_warehouse_id,
    moved_at_rank,
    qty_avg_1_prior,
    last_qty,
    warehouse_capacity_vs_move_type_warehouse_opened,
    last_known_qty,
    rows_in_move_type_sku_is_active,
    warehouse_capacity_avg_4,
    minutes_since_prev_moved_at_2,
    min_warehouse_capacity_so_far,
    warehouse_capacity_share_of_move_type_warehouse_,
    sku_is_active_flag,
    CASE WHEN qty_avg_1_prior >= 300.4 THEN 'HIGH' WHEN qty_avg_1_prior >= 200.6 THEN 'MEDIUM' ELSE 'LOW' END AS qty_avg_1_prior_tier
FROM annotated
