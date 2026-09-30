WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(unit_price AS INT) AS unit_price_int,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM order_items
    WHERE product_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        quantity_level,
        COALESCE(unit_price_int, 0) AS unit_price_int_filled
    FROM cleaned
    WHERE product_id IS NOT NULL
      AND quantity >= 8
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price / NULLIF(unit_price_int_filled, 0) AS unit_price_per_unit_price_int_filled
    FROM base
    WHERE product_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_int_filled DESC, id ASC) = 1
),
cleaned_order_items AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        quantity_level,
        unit_price_int_filled,
        unit_price_per_unit_price_int_filled,
        unit_price_per_unit_price_int_filled - unit_price AS unit_price_per_unit_price_int_filled_minus_unit_price,
        unit_price_per_unit_price_int_filled + unit_price_int_filled AS unit_price_per_unit_price_int_filled_plus_unit_price_int_filled
    FROM ranked
    WHERE unit_price_int_filled > 80
)
-- Grouped metrics
SELECT
    CASE WHEN unit_price >= 390 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    SUM(unit_price_per_unit_price_int_filled_plus_unit_price_int_filled) AS total_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled,
    APPROX_PERCENTILE(unit_price_per_unit_price_int_filled, ARRAY(0.25, 0.5, 0.75)) AS unit_price_per_unit_price_int_filled_quartiles,
    ROUND(AVG(unit_price_per_unit_price_int_filled_plus_unit_price_int_filled), 2) AS avg_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled
FROM cleaned_order_items
GROUP BY CASE WHEN unit_price >= 390 THEN 'LARGE' ELSE 'SMALL' END
HAVING SUM(unit_price_per_unit_price_int_filled_plus_unit_price_int_filled) > 0
ORDER BY avg_unit_price_per_unit_price_int_filled_plus_unit_price_int_filled DESC NULLS LAST
