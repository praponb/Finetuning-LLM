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
    WHERE qty <= 300
),
enriched_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM scoped_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE NOT ARRAY_CONTAINS(sk.labels, 'fragile')
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched_sku s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.sku_id IS NOT NULL
        )
)
SELECT
    operator,
    sku_id,
    SUM(CASE WHEN operator IS NOT NULL THEN sku_weight_kg ELSE 0 END) AS sku_weight_kg_operator_is_not_null,
    ROUND(100 * COUNT(CASE WHEN operator <> 'op_eve' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_eve,
    MIN(unit_price) AS min_unit_price
FROM with_activity
GROUP BY GROUPING SETS ((operator, sku_id), (operator))
ORDER BY operator NULLS LAST, sku_id NULLS LAST
