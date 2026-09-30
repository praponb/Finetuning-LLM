WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        SUBSTRING(TRIM(operator), 1, 3) AS operator_prefix,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-24'
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND HOUR(st.moved_at) BETWEEN 8 AND 10
              AND st.unit_price <= 323.9
        )
),
with_details AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.operator_prefix,
        s.move_type_slug,
        sk.sku_code,
        sk.labels AS sku_labels
    FROM with_activity s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category NOT IN ('Toys', 'Electronics')
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    operator_prefix,
    move_type_slug,
    sku_code,
    sku_labels,
    CONCAT_WS('-', sku_code, move_type_slug) AS sku_code_move_type_slug_label,
    CONCAT_WS(' / ', operator, operator_prefix) AS operator_operator_prefix_label
FROM with_details
WHERE ARRAY_CONTAINS(sku_labels, 'express')
ORDER BY operator_operator_prefix_label, operator_prefix DESC, id
LIMIT 5
