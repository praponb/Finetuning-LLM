WITH cleaned AS (
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
        LENGTH(TRIM(operator)) AS operator_len,
        TO_DATE(moved_at) AS moved_at_date,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM stock_moves
    WHERE operator = 'op_dew'
      AND warehouse_id IS NOT NULL
),
exploded_detail AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_date,
        s.operator_or_unknown,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.warehouse_id AS move_warehouse_id
    FROM cleaned s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Join stock_moves
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.moved_at_date,
        s.operator_or_unknown,
        s.move_id,
        s.move_qty,
        s.move_unit_price,
        s.move_warehouse_id,
        st.operator AS move_operator
    FROM exploded_detail s
    JOIN stock_moves st ON st.id = s.move_id
),
joined_sku AS (
    -- Join skus
    SELECT s.*, sk.weight_kg AS sku_weight_kg, w.opened_on AS warehouse_opened_on
    FROM enriched s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
operator_stats AS (
    -- Avg sku_weight_kg per operator
    SELECT operator, AVG(sku_weight_kg) AS avg_sku_weight_kg
    FROM joined_sku
    GROUP BY operator
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_sku_weight_kg,
        ROUND(s.sku_weight_kg - g.avg_sku_weight_kg, 2) AS sku_weight_kg_minus_avg
    FROM joined_sku s
    JOIN operator_stats g ON g.operator = s.operator
    WHERE s.sku_weight_kg > g.avg_sku_weight_kg
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at_date,
    move_id,
    move_qty,
    move_unit_price,
    move_operator,
    sku_weight_kg,
    avg_sku_weight_kg,
    sku_weight_kg_minus_avg,
    DATE_ADD(moved_at_date, 90) AS moved_at_date_plus_90d
FROM outliers
ORDER BY move_type NULLS LAST, avg_sku_weight_kg
