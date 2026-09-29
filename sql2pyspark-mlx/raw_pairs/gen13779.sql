WITH filtered AS (
    SELECT
        *,
        ROUND(price / cost, 2) AS margin,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE launch_date >= TO_DATE('2023-07-18')
      AND brand IS NOT DISTINCT FROM 'Initech'
)
SELECT *
FROM (
    SELECT
        supplier_id,
        brand,
        category,
        price
    FROM filtered
) src
PIVOT (
    MAX(price)
    FOR category IN ('Grocery', 'Electronics', 'Fashion')
)
