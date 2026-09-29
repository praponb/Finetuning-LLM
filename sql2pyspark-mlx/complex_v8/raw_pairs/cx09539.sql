WITH scoped_moves AS (
    -- Filter stock_moves
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
    WHERE unit_price <= 307.8
      AND warehouse_id >= 3
),
with_parent AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        sk.category AS sku_category,
        sk.unit_cost AS sku_unit_cost
    FROM scoped_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN st.move_type IS NOT NULL THEN 1 END) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_move_type_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS avg_move_unit_price
    FROM with_parent s
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(qty) OVER (ORDER BY move_count, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS qty_max_6_prior,
        MIN(warehouse_capacity) OVER (PARTITION BY move_count) AS move_count_min_warehouse_capacity,
        ROW_NUMBER() OVER w_all_by_move_count AS seq_no,
        DENSE_RANK() OVER (ORDER BY move_count) AS move_count_dense_rank,
        COUNT(*) OVER (PARTITION BY move_count) AS rows_in_move_count
    FROM with_counts
    WINDOW w_all_by_move_count AS (ORDER BY move_count, id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_city, move_count, sku_category, warehouse_capacity
    FROM leaders
) src
PIVOT (
    SUM(warehouse_capacity)
    FOR sku_category IN ('Toys', 'Beauty')
)
