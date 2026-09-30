SELECT suppliers.country, MIN(products.launch_date) AS min_launch_date
FROM products
RIGHT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
WHERE suppliers.city = 'London'
GROUP BY suppliers.country
ORDER BY min_launch_date;
