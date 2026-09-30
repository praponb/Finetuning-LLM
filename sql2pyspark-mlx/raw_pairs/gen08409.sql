SELECT p.id, p.category, p.cost, p.brand FROM products p WHERE p.cost < (SELECT AVG(p2.cost) FROM products p2 WHERE p2.category = p.category) ORDER BY p.cost
