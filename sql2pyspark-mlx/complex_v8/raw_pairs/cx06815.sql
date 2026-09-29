WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        qty - unit_price AS qty_minus_unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM stock_moves
    WHERE operator IS NOT NULL
      AND moved_at < TIMESTAMP '2025-03-02 10:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        unit_price_filled + qty_minus_unit_price AS unit_price_filled_plus_qty_minus_unit_price
    FROM cleaned_moves
),
joined AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.qty_minus_unit_price,
        s.moved_at_label,
        s.unit_price_filled_plus_qty_minus_unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category,
        sk.weight_kg AS sku_weight_kg
    FROM labeled s
    JOIN skus sk ON sk.id = s.sku_id
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(sku_weight_kg / NULLIF(qty_minus_unit_price, 0), 3) AS sku_weight_kg_per_qty_minus_unit_price,
        LOWER(TRIM(moved_at_label)) AS moved_at_label_clean
    FROM joined
    WHERE moved_at_label IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    sku_id,
    warehouse_id,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
    ROUND(SUM(unit_price), 2) AS total_unit_price
FROM labeled_2
GROUP BY ROLLUP(sku_id, warehouse_id)
