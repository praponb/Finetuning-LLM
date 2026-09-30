SELECT p.id, p.category, p.price FROM products p WHERE p.price = (SELECT MIN(p2.price) FROM products p2 WHERE p2.category = p.category);
