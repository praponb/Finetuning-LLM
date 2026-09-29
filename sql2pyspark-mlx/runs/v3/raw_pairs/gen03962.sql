SELECT
    store_sales.id,
    store_sales.product_id,
    store_sales.store_id,
    products.cost,
    products.brand,
    suppliers.supplier_name,
    suppliers.city
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
WHERE suppliers.city = 'New York' OR store_sales.store_id = 1
ORDER BY store_sales.product_id;
