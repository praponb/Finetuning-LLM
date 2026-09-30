WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        DATE_TRUNC('MONTH', moved_at) AS moved_at_month
    FROM stock_moves
    WHERE (TO_DATE(moved_at) < DATE '2025-03-02' OR operator IS NOT NULL)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator,
        unit_price,
        move_type_ref,
        CASE WHEN operator IN ('op_dew', 'op_chan') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label
    FROM scoped
    WHERE move_type = 'INBOUND'
),
enriched_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.unit_price,
        s.operator_group,
        s.move_type_operator_label,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code,
        sk.unit_cost AS sku_unit_cost,
        sk.weight_kg AS sku_weight_kg
    FROM enriched s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity > 4912
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(sku_weight_kg / NULLIF(SUM(sku_weight_kg) OVER w_operator, 0), 4) AS sku_weight_kg_share_of_operator,
        ROUND(warehouse_capacity / NULLIF(SUM(warehouse_capacity) OVER (PARTITION BY operator), 0), 4) AS warehouse_capacity_share_of_operator
    FROM enriched_warehouse
    WINDOW w_operator AS (PARTITION BY operator)
)
SELECT
    sku_id,
    warehouse_id,
    GROUPING(sku_id) AS is_total_sku_id,
    MIN(warehouse_capacity) AS min_warehouse_capacity
FROM windowed
GROUP BY ROLLUP(sku_id, warehouse_id)
