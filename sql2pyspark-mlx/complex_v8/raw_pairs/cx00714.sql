WITH base_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        qty - unit_price AS qty_minus_unit_price,
        ABS(unit_price - 215.4) AS unit_price_distance
    FROM stock_moves
    WHERE qty < 332
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity
    FROM base_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE UPPER(TRIM(sk.sku_code)) LIKE 'T%'
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.sku_unit_cost,
        s.warehouse_opened_on,
        s.warehouse_capacity,
        w.opened_on AS warehouse_opened_on_2,
        w.city AS warehouse_city,
        sk.weight_kg AS sku_weight_kg
    FROM combined_details s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.wh_code IS NOT NULL
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(sku_weight_kg) OVER (ORDER BY warehouse_opened_on DESC, id) AS prev_sku_weight_kg,
        COUNT(*) OVER (PARTITION BY warehouse_city) AS rows_in_warehouse_city
    FROM with_details_warehouse
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        FLOOR(prev_sku_weight_kg / 5) * 5 AS prev_sku_weight_kg_bucket
    FROM sequenced
)
SELECT DISTINCT warehouse_capacity
FROM labeled
ORDER BY warehouse_capacity DESC
