WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        days_since_moved_at,
        qty_per_unit_price,
        ROUND(qty_per_unit_price / NULLIF(SUM(qty_per_unit_price) OVER w_move_type_sku_id, 0), 4) AS qty_per_unit_price_share_of_move_type_sku_id,
        NTILE(10) OVER w_warehouse_id_by_days_since_m AS days_since_moved_at_decile,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY move_type, sku_id)) AS distinct_operator_in_move_type_sku_id,
        ROUND(AVG(days_since_moved_at) OVER (PARTITION BY move_type, sku_id), 2) AS move_type_sku_id_avg_days_since_moved_at
    FROM base
    WINDOW w_move_type_sku_id AS (PARTITION BY move_type, sku_id), w_warehouse_id_by_days_since_m AS (PARTITION BY warehouse_id ORDER BY days_since_moved_at NULLS LAST, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        qty,
        moved_at,
        qty_per_unit_price_share_of_move_type_sku_id,
        days_since_moved_at_decile,
        move_type_sku_id_avg_days_since_moved_at,
        CASE WHEN days_since_moved_at >= 525 THEN 'HIGH' WHEN days_since_moved_at >= 175 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_moved_at_tier
    FROM metrics
    WHERE HOUR(moved_at) BETWEEN 7 AND 9
)
SELECT
    id,
    qty,
    moved_at,
    days_since_moved_at_decile,
    move_type_sku_id_avg_days_since_moved_at,
    days_since_moved_at_tier
FROM tagged
ORDER BY move_type_sku_id_avg_days_since_moved_at NULLS LAST
