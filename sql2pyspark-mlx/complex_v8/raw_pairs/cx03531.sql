WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj,
        YEAR(moved_at) AS moved_at_year,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
),
joined_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price_adj,
        s.moved_at_year,
        s.unit_price_bucket,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM valid s
    JOIN warehouses w ON w.id = s.warehouse_id
),
joined_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price_adj,
        s.moved_at_year,
        s.warehouse_wh_code,
        sk.category AS sku_category,
        sk.sku_code
    FROM joined_warehouse s
    JOIN skus sk ON sk.id = s.sku_id
),
joined_warehouse_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.unit_price_adj,
        s.moved_at_year,
        s.warehouse_wh_code,
        s.sku_code,
        w.wh_code AS warehouse_wh_code_2,
        w.opened_on AS warehouse_opened_on
    FROM joined_sku s
    JOIN warehouses w ON w.id = s.warehouse_id AND LOWER(w.wh_code) LIKE '%k1%'
    WHERE w.opened_on BETWEEN DATE '2024-04-13' AND DATE '2025-07-03'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', warehouse_wh_code_2, sku_code) AS warehouse_wh_code_2_sku_code_label
    FROM joined_warehouse_2
    WHERE UPPER(TRIM(warehouse_wh_code)) LIKE 'B%'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    unit_price_adj,
    moved_at_year,
    warehouse_wh_code,
    sku_code,
    warehouse_wh_code_2,
    warehouse_opened_on,
    warehouse_wh_code_2_sku_code_label
FROM labeled
WHERE warehouse_wh_code_2 IS NOT NULL
ORDER BY warehouse_wh_code DESC NULLS FIRST, warehouse_id
