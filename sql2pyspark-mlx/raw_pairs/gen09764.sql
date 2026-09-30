SELECT p.country, p.city, e.category, COUNT(*) AS rows_total, ROUND(AVG(e.price), 2) AS avg_price FROM products e JOIN suppliers p ON e.supplier_id = p.id GROUP BY p.country, p.city, e.category;
