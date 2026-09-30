WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        DATEDIFF(CURRENT_DATE(), launch_date) / 365 AS years_since_launch_date,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2023-06-28'
      AND cost >= 210
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    brand,
    kpi,
    val
FROM (
    SELECT
        id,
        brand,
        years_since_launch_date,
        cost
    FROM cleaned
) src
UNPIVOT (
    val FOR kpi IN (years_since_launch_date AS YEARS_SINCE_LAUNCH_DATE_VAL, cost)
)
