WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        CASE WHEN unit_price >= 329.7 THEN 'A' WHEN unit_price >= 136.0 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND unit_price <= 367.7
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_tier,
        SUM(qty) OVER (PARTITION BY unit_price_tier) AS unit_price_tier_sum_qty,
        MIN(unit_price) OVER w_unit_price_tier AS unit_price_tier_min_unit_price
    FROM valid_moves
    WINDOW w_unit_price_tier AS (PARTITION BY unit_price_tier)
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty > 91
              AND st.operator <> 'op_eve'
        )
),
deduped AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        unit_price_tier,
        unit_price_tier_sum_qty,
        unit_price_tier_min_unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY unit_price_tier_min_unit_price DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.moved_at,
        s.unit_price,
        s.unit_price_tier,
        s.unit_price_tier_sum_qty,
        s.unit_price_tier_min_unit_price,
        st.id AS move_id,
        st.qty AS move_qty,
        st.operator AS move_operator,
        st.moved_at AS move_moved_at
    FROM deduped s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND HOUR(st.moved_at) BETWEEN 7 AND 11
),
joined_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.moved_at,
        s.unit_price,
        s.unit_price_tier,
        s.unit_price_tier_sum_qty,
        s.unit_price_tier_min_unit_price,
        s.move_id,
        s.move_qty,
        s.move_operator,
        s.move_moved_at,
        w.opened_on AS warehouse_opened_on,
        st.move_type,
        st.operator AS move_operator_2
    FROM with_children_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN stock_moves st ON st.id = s.move_id
    WHERE w.opened_on >= DATE '2024-04-13'
)
-- Final projection
SELECT
    id,
    moved_at,
    unit_price,
    unit_price_tier,
    unit_price_tier_sum_qty,
    unit_price_tier_min_unit_price,
    move_id,
    move_qty,
    move_operator,
    warehouse_opened_on,
    move_type,
    move_operator_2,
    QUARTER(move_moved_at) AS move_moved_at_quarter,
    CASE WHEN unit_price_tier_sum_qty >= 3000 THEN 'large' WHEN unit_price_tier_sum_qty >= 2251 THEN 'medium' ELSE 'small' END AS unit_price_tier_sum_qty_tier
FROM joined_warehouse
ORDER BY unit_price_tier_min_unit_price, moved_at DESC
