SELECT
    store_sales.id,
    store_sales.store_id,
    store_sales.revenue,
    products.product_name,
    products.price,
    suppliers.supplier_name,
    suppliers.country
FROM store_sales
JOIN products ON products.id = store_sales.product_id
LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
