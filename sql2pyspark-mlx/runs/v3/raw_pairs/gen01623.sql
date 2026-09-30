SELECT products.brand, MIN(store_sales.sale_date) AS first_sale_date, MAX(store_sales.units) - MIN(store_sales.units) AS units_range
FROM store_sales
JOIN products ON store_sales.product_id = products.id
RIGHT JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY products.brand
ORDER BY 2, brand
LIMIT 50;
