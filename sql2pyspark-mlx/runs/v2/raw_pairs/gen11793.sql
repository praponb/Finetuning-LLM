SELECT p.id, p.category, p.cost, p.brand, s.rating FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id WHERE s.rating >= 2.7 ORDER BY s.rating DESC, category;
