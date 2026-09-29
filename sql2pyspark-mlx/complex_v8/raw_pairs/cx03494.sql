WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE unit_price > 119.5
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        moved_at_label,
        LENGTH(TRIM(moved_at_label)) AS moved_at_label_len,
        SUBSTRING(TRIM(move_type), 1, 3) AS move_type_prefix
    FROM valid
),
latest_per_key AS (
    -- Keep the latest row per warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        moved_at_label,
        moved_at_label_len,
        move_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY moved_at_label_len DESC, id DESC) AS rn
        FROM annotated
    ) ranked
    WHERE rn = 1
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active,
        sk.sku_code,
        w.capacity AS warehouse_capacity
    FROM latest_per_key s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT id, metric, reading
FROM (
    SELECT id, unit_price, sku_weight_kg
    FROM with_details_sku
) src
UNPIVOT (
    reading FOR metric IN (unit_price, sku_weight_kg)
) unp
