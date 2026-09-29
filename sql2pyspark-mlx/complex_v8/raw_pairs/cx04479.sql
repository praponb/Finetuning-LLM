WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE qty BETWEEN 61 AND 370
),
combined_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        s.move_type_or_unknown,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity,
        sk.category AS sku_category
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_category, warehouse_id), 2) AS unit_price_vs_sku_category_warehouse_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY operator ORDER BY qty ASC), 3) AS qty_percent_rank,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_category, warehouse_id), 0), 4) AS qty_share_of_sku_category_warehouse_id
    FROM combined_details
)
-- Final projection
SELECT
    id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price,
    move_type_slug,
    warehouse_city,
    warehouse_wh_code,
    warehouse_capacity,
    unit_price_vs_sku_category_warehouse_id_avg,
    qty_percent_rank
FROM windowed
ORDER BY id DESC, operator
