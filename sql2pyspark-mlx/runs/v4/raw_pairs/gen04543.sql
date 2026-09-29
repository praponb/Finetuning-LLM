WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(product_name, 1, 2) product_name_prefix,
        SIZE(tags) tags_count,
        COALESCE(brand, 'UNKNOWN') brand_filled
    FROM products
    WHERE launch_date >= TO_DATE('2023-11-27')
      AND supplier_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        brand_filled,
        WEEKOFYEAR(launch_date) launch_date_weekofyear
    FROM filtered
    WHERE launch_date >= TO_DATE('2023-12-19')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY cost DESC, id) row_seq,
        MIN(price) OVER (PARTITION BY supplier_id) supplier_id_min_price
    FROM cleaned
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        launch_date_weekofyear
    FROM scored
) src
PIVOT (
    MAX(launch_date_weekofyear)
    FOR category IN ('Electronics', 'Home', 'Books')
)
