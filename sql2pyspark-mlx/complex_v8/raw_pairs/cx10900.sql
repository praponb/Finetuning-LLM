WITH valid_moves AS (
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
    WHERE operator IS NOT NULL
),
with_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.category AS sku_category
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        move_type,
        qty,
        unit_price,
        ROUND(CUME_DIST() OVER (ORDER BY qty ASC), 3) AS qty_cume_dist,
        SUM(qty) OVER (PARTITION BY sku_id) AS sku_id_sum_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id) AS distinct_warehouse_id_in_sku_id,
        ROUND(CUME_DIST() OVER (PARTITION BY sku_category, sku_id ORDER BY unit_price DESC, id), 3) AS unit_price_cume_dist,
        NTH_VALUE(qty, 2) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_qty,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        ROUND(CUME_DIST() OVER w_all_by_moved_at_ties, 3) AS moved_at_cume_dist,
        unit_price - LAG(unit_price) OVER (ORDER BY qty ASC, id) AS unit_price_change,
        ROUND(AVG(qty) OVER (PARTITION BY sku_category), 2) AS sku_category_avg_qty,
        NTILE(5) OVER (ORDER BY unit_price, id) AS unit_price_quintile,
        SUM(qty) OVER w_sku_category AS sku_category_sum_qty
    FROM with_details_sku
    WINDOW w_sku_id AS (PARTITION BY sku_id), w_all_by_moved_at_ties AS (ORDER BY moved_at DESC), w_sku_category AS (PARTITION BY sku_category)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(move_type, '#', CAST(unit_price_quintile AS STRING)) AS move_type_ref,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
    FROM with_trends
    WHERE last_known_unit_price > 160.8
)
-- Final projection
SELECT
    id,
    move_type,
    qty,
    unit_price,
    qty_cume_dist,
    sku_id_sum_qty,
    distinct_warehouse_id_in_sku_id,
    unit_price_cume_dist,
    second_qty,
    last_known_unit_price,
    moved_at_cume_dist,
    unit_price_change,
    sku_category_sum_qty,
    qty_per_unit_price,
    LOWER(TRIM(move_type_ref)) AS move_type_ref_clean
FROM derived
WHERE move_type_ref IS NOT NULL
ORDER BY sku_category_sum_qty DESC, id
LIMIT 20
