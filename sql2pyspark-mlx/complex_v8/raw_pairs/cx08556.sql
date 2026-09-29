WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND sku_id IS NOT NULL
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.days_since_moved_at,
        s.moved_at_label,
        sk.sku_code,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg,
        w.capacity AS warehouse_capacity
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.moved_at_label,
        s.sku_code,
        s.sku_category,
        s.sku_weight_kg,
        s.warehouse_capacity,
        w.opened_on AS warehouse_opened_on
    FROM enriched s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity BETWEEN 4912 AND 11768
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost,
        w.opened_on AS warehouse_opened_on_2,
        w.wh_code AS warehouse_wh_code
    FROM joined_warehouse s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        sku_category,
        CASE WHEN warehouse_capacity >= 11506 THEN 'A' WHEN warehouse_capacity >= 4912 THEN 'B' ELSE 'C' END AS warehouse_capacity_tier,
        DATE_TRUNC('MONTH', warehouse_opened_on_2) AS warehouse_opened_on_2_month,
        SUM(sku_weight_kg) AS total_sku_weight_kg,
        SUM(warehouse_capacity) AS total_warehouse_capacity,
        COUNT(*) AS cnt
    FROM enriched_2
    WHERE warehouse_capacity < 11005
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    sku_category,
    total_sku_weight_kg,
    total_warehouse_capacity,
    ROUND(total_sku_weight_kg / NULLIF(total_warehouse_capacity, 0), 3) AS total_sku_weight_kg_per_total_warehouse_capacity,
    FLOOR(total_sku_weight_kg / 25) * 25 AS total_sku_weight_kg_bucket
FROM rollup_base
ORDER BY total_warehouse_capacity, total_sku_weight_kg_per_total_warehouse_capacity DESC
