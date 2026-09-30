WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        LOWER(TRIM(operator)) AS operator_clean,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        operator_clean,
        move_type_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY qty DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM latest_per_key s
    WHERE s.sku_id IN (
            SELECT st.sku_id
            FROM stock_moves st
            WHERE st.move_type IN ('ADJUST', 'OUTBOUND')
        )
      AND s.unit_price > 152.7
)
-- Final projection
SELECT
    id,
    warehouse_id,
    move_type,
    operator,
    unit_price,
    operator_clean,
    move_type_ref,
    GREATEST(unit_price, qty) AS greatest_unit_price_qty,
    ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price
FROM qualified
WHERE move_type_ref IS NOT NULL
ORDER BY operator, id
LIMIT 10
