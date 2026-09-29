WITH product_totals AS (
    SELECT product_id, ROUND(AVG(units), 2) AS avg_units, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT
    products.id,
    products.category,
    product_totals.avg_units,
    product_totals.avg_revenue
FROM products
JOIN product_totals ON products.id = product_totals.product_id;
