WITH scoped AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND warehouse_id >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty,
        ROUND(unit_price / NULLIF(qty, 0), 3) AS unit_price_per_qty
    FROM scoped
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        operator,
        CASE WHEN qty >= 245 THEN 'large' WHEN qty >= 168 THEN 'medium' ELSE 'small' END AS qty_tier,
        SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix,
        UPPER(TRIM(operator)) AS operator_clean
    FROM labeled
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    operator,
    qty_tier,
    operator_prefix,
    operator_clean,
    INITCAP(TRIM(operator_clean)) AS operator_clean_clean,
    LOWER(REGEXP_REPLACE(TRIM(operator_prefix), ' ', '_')) AS operator_prefix_slug
FROM labeled_2
WHERE warehouse_id IS NOT NULL
ORDER BY id DESC
