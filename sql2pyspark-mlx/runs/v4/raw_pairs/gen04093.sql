WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        launch_date,
        tags,
        ARRAY_CONTAINS(tags, 'premium') AS is_premium,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2024-02-28'
      AND brand IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS dedup_rank
    FROM base
) ranked
WHERE dedup_rank = 1
