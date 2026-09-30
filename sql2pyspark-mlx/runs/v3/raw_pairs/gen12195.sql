SELECT
    store_sales.id,
    store_sales.revenue,
    products.supplier_id,
    suppliers.city,
    suppliers.rating,
    ROUND(store_sales.units, 1) units_rounded
FROM store_sales
INNER JOIN products ON products.id = store_sales.product_id
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
WHERE store_sales.revenue BETWEEN 1500 AND 2400;
