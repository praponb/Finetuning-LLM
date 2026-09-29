WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATE_TRUNC('DAY', moved_at) AS moved_at_day
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
deduped AS (
    -- Keep the earliest row per operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        moved_at_day
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator ORDER BY moved_at_day, id) AS row_num
        FROM base_moves
    ) ranked
    WHERE row_num = 1
),
with_parent AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.moved_at_day,
        sk.is_active AS sku_is_active,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels
    FROM deduped s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    moved_at,
    sku_is_active,
    sku_unit_cost,
    DATEDIFF(DATE '2025-12-31', moved_at_day) AS days_since_moved_at_day
FROM with_parent
WHERE moved_at >= TIMESTAMP '2025-01-13 09:00:00'
