WITH store_sales_by_product AS (
    SELECT product_id, COUNT(DISTINCT revenue) AS unique_products, AVG(units) AS avg_units
    FROM store_sales
    WHERE revenue >= 3300
    GROUP BY product_id
)
SELECT
    p.category,
    p.product_name,
    ssbp.unique_products,
    ssbp.avg_units
FROM products p
INNER JOIN store_sales_by_product ssbp ON p.id = ssbp.product_id
WHERE ssbp.avg_units > 36;
