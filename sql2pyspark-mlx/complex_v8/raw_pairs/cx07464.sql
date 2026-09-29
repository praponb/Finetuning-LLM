WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE qty >= 191
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.is_active AS sku_is_active,
        sk.labels AS sku_labels,
        w.capacity AS warehouse_capacity,
        w.city AS warehouse_city
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_details_sku
    WHERE unit_price > (SELECT MIN(unit_price) FROM with_details_sku)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        sku_is_active,
        sku_labels,
        SIZE(sku_labels) AS sku_labels_count
    FROM filtered
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.sku_is_active,
        s.sku_labels_count,
        sk.labels AS sku_labels_2
    FROM derived s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.sku_code IS NOT NULL
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        qty,
        operator,
        sku_is_active,
        sku_labels_2,
        ARRAY_CONTAINS(sku_labels_2, 'bulky') AS has_bulky,
        ROUND(qty / NULLIF(sku_labels_count, 0), 3) AS qty_per_sku_labels_count
    FROM joined_sku
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        qty,
        operator,
        sku_is_active,
        sku_labels_2,
        qty_per_sku_labels_count,
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM scored
)
SELECT
    id,
    sku_id,
    operator,
    sku_is_active,
    sku_labels_2,
    qty_per_sku_labels_count,
    qty_bucket
FROM derived_2
WHERE operator_or_unknown = 'op_chan'
