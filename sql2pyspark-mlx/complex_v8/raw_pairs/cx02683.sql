WITH valid_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(warehouse_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND qty >= 152
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, sk.is_active AS sku_is_active
    FROM valid_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id AND sk.weight_kg > 8.5
),
latest AS (
    -- Keep the latest row per sku_id, operator
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        operator_ref,
        sku_is_active
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY moved_at DESC, id DESC) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Derive helper columns
    SELECT *, IF(sku_is_active, 'yes', 'no') AS sku_is_active_label
    FROM latest
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM derived
    WHERE unit_price < (SELECT MAX(unit_price) FROM derived)
)
SELECT *
FROM (
    SELECT warehouse_id, sku_is_active, operator, qty
    FROM filtered
) src
PIVOT (
    MAX(qty)
    FOR operator IN ('op_eve', 'op_ann')
)
ORDER BY warehouse_id, sku_is_active
