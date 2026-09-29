SELECT suppliers.country, MIN(products.launch_date) AS first_launch_date, MAX(products.stock_qty) - MIN(products.stock_qty) AS stock_qty_range
FROM products
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY suppliers.country
ORDER BY country DESC
