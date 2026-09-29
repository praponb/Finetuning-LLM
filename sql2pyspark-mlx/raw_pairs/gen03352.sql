SELECT a.id, a.category, a.cost FROM products a WHERE a.cost > (SELECT AVG(b.cost) FROM products b WHERE b.category = a.category) ORDER BY a.category;
