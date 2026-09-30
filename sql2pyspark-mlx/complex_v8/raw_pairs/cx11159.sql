WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(operator)) AS operator_len,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        CASE WHEN qty >= 387 THEN 'A' WHEN qty >= 123 THEN 'B' ELSE 'C' END AS qty_tier
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(operator_len, 0) AS operator_len_filled,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
    FROM base
    WHERE warehouse_id >= 4
),
joined AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg,
        sk.unit_cost AS sku_unit_cost
    FROM derived s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category <> 'Food'
    WHERE sk.unit_cost <= 251.2
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price_bucket,
        s.qty_tier,
        s.operator_len_filled,
        s.unit_price_adj,
        s.operator_or_unknown,
        s.sku_labels,
        s.sku_weight_kg,
        s.sku_unit_cost,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.category AS sku_category,
        sk.labels AS sku_labels_2
    FROM joined s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    moved_at,
    operator_len_filled,
    sku_labels,
    sku_weight_kg,
    warehouse_wh_code,
    warehouse_opened_on,
    warehouse_city,
    sku_category,
    TRUNC(warehouse_opened_on, 'MM') AS warehouse_opened_on_month_start
FROM with_parent_warehouse
ORDER BY warehouse_wh_code DESC, moved_at DESC NULLS FIRST
