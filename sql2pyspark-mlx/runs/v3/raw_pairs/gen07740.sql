SELECT p.id, p.cost, t.avg_units FROM products p LEFT JOIN (SELECT product_id, ROUND(AVG(units), 2) AS avg_units FROM store_sales GROUP BY product_id) t ON p.id = t.product_id WHERE t.avg_units > 64;
