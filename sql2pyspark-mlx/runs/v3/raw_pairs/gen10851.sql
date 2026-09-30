SELECT
    store_sales.revenue,
    store_sales.units,
    products.product_name,
    suppliers.city
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
RIGHT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.category = 'Home'
ORDER BY store_sales.units;
