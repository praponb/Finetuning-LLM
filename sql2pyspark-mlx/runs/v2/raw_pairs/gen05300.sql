SELECT suppliers.country, suppliers.city, COUNT(DISTINCT products.cost) unique_categorys
FROM products
RIGHT JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.launch_date BETWEEN '2024-09-05' AND '2024-11-01'
GROUP BY suppliers.country, suppliers.city
HAVING COUNT(DISTINCT products.cost) > 3
