SELECT suppliers.city, ROUND(AVG(products.cost), 2) AS avg_cost FROM products INNER JOIN suppliers ON products.supplier_id = suppliers.id GROUP BY suppliers.city ORDER BY avg_cost DESC, city LIMIT 5;
