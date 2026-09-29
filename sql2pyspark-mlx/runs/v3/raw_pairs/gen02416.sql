SELECT s.city, COUNT(CASE WHEN p.category = 'Books' THEN 1 END) AS books_count
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.city
ORDER BY city;
