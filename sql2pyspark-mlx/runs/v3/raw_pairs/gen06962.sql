SELECT
    id,
    price,
    category,
    product_name,
    CASE WHEN stock_qty >= 338 THEN 'High' WHEN stock_qty >= 168 THEN 'Medium' ELSE 'Low' END AS stock_qty_tier
FROM products
ORDER BY product_name;
