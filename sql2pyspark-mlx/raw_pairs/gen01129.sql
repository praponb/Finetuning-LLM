SELECT
    products.id,
    products.brand,
    t.units_range,
    t.revenue_count
FROM products
INNER JOIN (SELECT product_id, MAX(units) - MIN(units) AS units_range, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.revenue_count > 6
ORDER BY products.id ASC, revenue_count ASC
LIMIT 10;
