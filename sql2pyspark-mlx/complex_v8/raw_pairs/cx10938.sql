WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 8 AND 10
      AND move_type IN ('INBOUND', 'ADJUST')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(unit_price_filled, 2) OVER (ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_filled,
        DENSE_RANK() OVER (ORDER BY unit_price_filled, id) AS unit_price_filled_dense_rank,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        MIN(qty) OVER (PARTITION BY sku_id, warehouse_id) AS sku_id_warehouse_id_min_qty,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, warehouse_id), 2) AS sku_id_warehouse_id_avg_qty,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY unit_price_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        LEAD(qty, 2) OVER (ORDER BY unit_price_filled DESC, id) AS next2_qty,
        RANK() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price DESC) AS unit_price_rank,
        ROW_NUMBER() OVER (PARTITION BY sku_id, warehouse_id ORDER BY unit_price DESC, id) AS rn,
        ROUND(unit_price_filled / NULLIF(SUM(unit_price_filled) OVER w_sku_id_warehouse_id, 0), 4) AS unit_price_filled_share_of_sku_id_warehouse_id,
        LEAD(moved_at, 3) OVER (PARTITION BY sku_id, operator ORDER BY unit_price_filled DESC NULLS FIRST, id) AS next3_moved_at,
        COUNT(*) OVER (PARTITION BY sku_id, warehouse_id) AS rows_in_sku_id_warehouse_id,
        ROUND(CUME_DIST() OVER w_sku_id_warehouse_id_by_unit_3, 3) AS unit_price_cume_dist
    FROM filtered_moves
    WINDOW w_sku_id_warehouse_id AS (PARTITION BY sku_id, warehouse_id), w_sku_id_warehouse_id_by_unit_3 AS (PARTITION BY sku_id, warehouse_id ORDER BY unit_price DESC, id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_filled,
    move_type_or_unknown,
    first_known_moved_at,
    sku_id_warehouse_id_min_qty,
    last_known_moved_at,
    unit_price_rank,
    unit_price_filled_share_of_sku_id_warehouse_id,
    next3_moved_at,
    unit_price_cume_dist
FROM ranked
ORDER BY next3_moved_at DESC NULLS FIRST, id
LIMIT 10
