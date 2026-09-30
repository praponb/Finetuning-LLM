WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE store_id = 4
),
agg AS (
    SELECT product_id, COUNT(units) AS units_count, COUNT(DISTINCT units) AS unique_products
    FROM filtered_store_sales
    GROUP BY product_id
)
SELECT product_id, units_count, unique_products
FROM agg
