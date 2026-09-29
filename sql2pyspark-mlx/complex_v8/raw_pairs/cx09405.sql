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
        unit_price,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE qty >= 78
      AND sku_id IS NOT NULL
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, sk.weight_kg AS sku_weight_kg
    FROM valid_moves s
    JOIN skus sk ON sk.id = s.sku_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_or_unknown,
        w.opened_on AS warehouse_opened_on
    FROM enriched s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LENGTH(TRIM(operator)) AS operator_len,
        ROUND(unit_price * 1.07, 2) AS unit_price_adj
    FROM combined_details
    WHERE move_type_or_unknown = 'ADJUST'
),
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price,
        s.move_type_or_unknown,
        s.warehouse_opened_on,
        s.unit_price_filled,
        s.operator_len,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        w.capacity AS warehouse_capacity
    FROM tagged s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT move_type_or_unknown) AS distinct_move_type_or_unknowns,
        SUM(CASE WHEN sku_category = 'Apparel' THEN warehouse_capacity ELSE 0 END) AS warehouse_capacity_sku_category_apparel,
        MIN(warehouse_opened_on) AS first_warehouse_opened_on,
        SUM(CASE WHEN sku_category = 'Apparel' THEN unit_price ELSE 0 END) AS unit_price_sku_category_apparel,
        MAX(moved_at) AS last_moved_at
    FROM with_details_sku
    GROUP BY 1
    HAVING AVG(unit_price) > 205.1
)
-- Final projection
SELECT
    sku_id,
    distinct_move_type_or_unknowns,
    warehouse_capacity_sku_category_apparel,
    first_warehouse_opened_on,
    unit_price_sku_category_apparel,
    last_moved_at,
    HOUR(last_moved_at) AS last_moved_at_hour
FROM rollup_base
ORDER BY unit_price_sku_category_apparel NULLS LAST
