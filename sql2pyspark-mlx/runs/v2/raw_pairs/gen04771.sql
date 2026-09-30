WITH ProductSummary AS (
    SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
    GROUP BY product_id
)
SELECT
    products.id,
    products.category,
    ProductSummary.avg_unit_price,
    ProductSummary.avg_quantity
FROM products
INNER JOIN ProductSummary ON products.id = ProductSummary.product_id
WHERE ProductSummary.avg_unit_price >= 29
ORDER BY 4 DESC, ProductSummary.avg_unit_price;
