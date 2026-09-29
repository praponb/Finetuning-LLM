WITH scoped_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket,
        CONCAT(move_type, '#', CAST(warehouse_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE qty < 426
      AND move_type IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(operator), 1, 2) AS operator_prefix,
        COALESCE(unit_price_bucket, 0) AS unit_price_bucket_filled
    FROM scoped_moves
),
latest_per_key AS (
    -- Keep the latest row per sku_id, move_type
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price_bucket,
        move_type_ref,
        operator_prefix,
        unit_price_bucket_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY qty DESC, id DESC) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    operator,
    unit_price_bucket,
    move_type_ref,
    unit_price_bucket_filled,
    UPPER(TRIM(operator)) AS operator_clean
FROM latest_per_key
