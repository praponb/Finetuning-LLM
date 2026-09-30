WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator
    FROM stock_moves
    WHERE warehouse_id >= 4
      AND move_type IS NOT NULL
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        MAX(unit_price) AS max_move_unit_price,
        COUNT(*) AS move_count,
        COUNT(*) AS move_count_2
    FROM stock_moves
    GROUP BY sku_id
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        st.max_move_unit_price,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_count_2, 0) AS move_count_2
    FROM joined_sku s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
)
-- Subtotals with GROUPING SETS
SELECT
    move_count,
    COALESCE(sku_category, 'ALL') AS sku_category,
    sku_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    SUM(max_move_unit_price) AS total_max_move_unit_price
FROM with_counts
GROUP BY GROUPING SETS ((move_count, sku_category, sku_id), (move_count))
ORDER BY move_count NULLS LAST, sku_category NULLS LAST, sku_id NULLS LAST
