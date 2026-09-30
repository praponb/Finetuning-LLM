WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE (sku_id IS NOT NULL OR moved_at >= TIMESTAMP '2025-01-25 10:00:00')
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        st.id AS move_id,
        st.operator AS move_operator,
        st.qty AS move_qty,
        st.move_type
    FROM filtered s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', move_operator, move_type) AS move_operator_move_type_label,
        SUBSTRING(TRIM(move_operator), 1, 3) AS move_operator_prefix,
        CONCAT(move_operator, '#', CAST(qty AS STRING)) AS move_operator_ref
    FROM exploded_detail_moves
    WHERE move_operator = 'op_dew'
)
SELECT
    sku_id,
    move_id,
    GROUPING(sku_id) AS is_total_sku_id,
    SUM(CASE WHEN move_type IN ('ADJUST', 'INBOUND') THEN 1 ELSE 0 END) AS n_move_type_in_adjust_inbound
FROM enriched
GROUP BY GROUPING SETS ((sku_id, move_id), (sku_id), (move_id), ())
