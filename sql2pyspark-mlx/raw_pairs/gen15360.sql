SELECT
    products.id,
    products.category,
    products.brand,
    t.store_sale_count,
    t.units_range
FROM products
LEFT OUTER JOIN (SELECT product_id, COUNT(*) AS store_sale_count, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    WHERE units > 37
    GROUP BY product_id) t ON products.id = t.product_id
