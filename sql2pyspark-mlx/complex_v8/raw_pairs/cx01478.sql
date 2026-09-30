WITH scoped AS (
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
        unit_price + qty AS unit_price_plus_qty,
        INITCAP(TRIM(move_type)) AS move_type_clean,
        ROUND(unit_price * 1.5, 2) AS unit_price_adj
    FROM stock_moves
    WHERE qty > 99
),
enriched_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.unit_price_adj,
        sk.weight_kg AS sku_weight_kg
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id AND SIZE(sk.labels) >= 1
    WHERE sk.weight_kg >= 4.7
),
filtered AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM enriched_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id <= 5
        )
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN qty >= 271 THEN 'A' WHEN qty >= 169 THEN 'B' ELSE 'C' END AS qty_tier,
        ROUND(unit_price_adj * 0.9, 2) AS unit_price_adj_adj,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM filtered
    WHERE move_type = 'TRANSFER'
)
SELECT
    id,
    warehouse_id,
    qty,
    unit_price,
    sku_weight_kg,
    qty_tier,
    unit_price_adj_adj,
    ROUND(sku_weight_kg / NULLIF(unit_price_adj, 0), 3) AS sku_weight_kg_per_unit_price_adj,
    LENGTH(TRIM(operator)) AS operator_len
FROM annotated
ORDER BY qty DESC
