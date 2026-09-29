WITH recent_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        is_active,
        CASE WHEN unit_cost >= 320.4 THEN 'HIGH' WHEN unit_cost >= 134.3 THEN 'MEDIUM' ELSE 'LOW' END AS unit_cost_tier
    FROM recent_skus
),
with_children_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.unit_cost,
        s.is_active,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price
    FROM annotated s
    JOIN stock_moves st ON st.sku_id = s.id AND st.unit_price BETWEEN 124.1 AND 361.9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(move_qty, 2, 0) OVER (PARTITION BY move_id, is_active ORDER BY move_qty, id, move_id) AS prev2_move_qty,
        ROUND(PERCENT_RANK() OVER (PARTITION BY move_id, is_active ORDER BY move_qty), 3) AS move_qty_percent_rank
    FROM with_children_moves
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.id) AS move_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS last_moved_at,
        (SELECT MAX(st.qty) FROM stock_moves st WHERE st.sku_id = s.id) AS max_move_qty
    FROM ranked s
),
with_parent_move AS (
    -- Join stock_moves
    SELECT
        s.id,
        s.category,
        s.unit_cost,
        s.is_active,
        s.move_id,
        s.move_qty,
        s.move_qty_percent_rank,
        s.move_count,
        s.last_moved_at,
        s.max_move_qty,
        st.sku_id AS move_sku_id,
        st.unit_price AS move_unit_price_2,
        st.move_type,
        w.wh_code AS warehouse_wh_code
    FROM with_stats s
    JOIN stock_moves st ON st.id = s.move_id
    JOIN warehouses w ON w.id = st.warehouse_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.id) AS first_moved_at
    FROM with_parent_move s
)
SELECT *
FROM (
    SELECT is_active, move_count, category, move_unit_price_2
    FROM summarized_rows
) src
PIVOT (
    MAX(move_unit_price_2) AS max_v, SUM(move_unit_price_2) AS sum_v
    FOR category IN ('Toys', 'Food')
)
