WITH prepared_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        sku_id,
        move_type,
        operator,
        move_type_slug,
        unit_price_bucket,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label,
        CONCAT(move_type_slug, '#', CAST(qty AS STRING)) AS move_type_slug_ref,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug_2
    FROM prepared_moves
)
-- Final projection
SELECT
    id,
    sku_id,
    move_type,
    operator,
    unit_price_bucket,
    move_type_slug_ref,
    move_type_slug_2,
    LENGTH(TRIM(move_type_slug_ref)) AS move_type_slug_ref_len,
    LOWER(TRIM(move_type_operator_label)) AS move_type_operator_label_clean
FROM tagged
ORDER BY move_type_slug_2
