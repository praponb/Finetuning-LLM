WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE move_type = 'INBOUND'
),
enriched_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.move_type_operator_label,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        w.city AS warehouse_city
    FROM filtered s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.sku_unit_cost,
        s.warehouse_city,
        st.id AS move_id,
        st.sku_id AS move_sku_id,
        st.qty AS move_qty,
        st.move_type AS move_move_type
    FROM enriched_sku s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.move_sku_id
              AND st.unit_price > 206.8
              AND HOUR(st.moved_at) BETWEEN 10 AND 12
        )
)
SELECT
    move_id,
    sku_id,
    ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
    ROUND(100 * COUNT(CASE WHEN move_move_type NOT IN ('TRANSFER', 'ADJUST') THEN 1 END) / COUNT(*), 1) AS pct_move_move_type_not_in_transf
FROM screened
GROUP BY GROUPING SETS ((move_id, sku_id), (move_id), ())
