SELECT s.country, COUNT(*) AS cnt FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE p.category = 'Home' GROUP BY s.country ORDER BY cnt DESC
