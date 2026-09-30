WITH cleaned_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE move_type IN ('OUTBOUND', 'INBOUND')
      AND sku_id IS NOT NULL
),
joined_sku AS (
    -- Join skus
    SELECT
        s.*,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        w.city AS warehouse_city
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
    WHERE ARRAY_CONTAINS(sk.labels, 'express')
),
latest_per_key AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        sku_unit_cost,
        sku_labels,
        warehouse_city
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY qty, id) AS recency_rank
        FROM joined_sku
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    sku_unit_cost,
    sku_labels,
    ROUND(qty / NULLIF(sku_unit_cost, 0), 3) AS qty_per_sku_unit_cost,
    COALESCE(operator, 'UNKNOWN') AS operator_or_unknown
FROM latest_per_key
ORDER BY qty_per_sku_unit_cost DESC NULLS FIRST, operator_or_unknown, id
LIMIT 5
