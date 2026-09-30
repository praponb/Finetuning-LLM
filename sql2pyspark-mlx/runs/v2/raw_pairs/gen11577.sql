SELECT s.country, SUM(CASE WHEN p.category = 'Books' THEN p.cost ELSE 0 END) AS books_cost, ROUND(STDDEV(p.price), 2) AS stddev_price
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY books_cost, country
LIMIT 20
