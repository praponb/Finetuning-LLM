WITH base AS (
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
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM base
    WHERE unit_price > (SELECT MIN(unit_price) FROM base)
),
with_parent_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.operator_len,
        sk.sku_code,
        w.opened_on AS warehouse_opened_on
    FROM screened s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE LOWER(sk.sku_code) LIKE '%l-%'
)
SELECT
    sku_id,
    operator_len,
    move_type,
    PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price,
    ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
    COUNT(*) AS n_rows
FROM with_parent_sku
GROUP BY CUBE(sku_id, operator_len, move_type)
