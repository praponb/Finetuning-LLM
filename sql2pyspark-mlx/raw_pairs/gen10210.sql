SELECT a.id, a.category, a.price, a.cost FROM products a WHERE a.price = (SELECT MAX(b.price) FROM products b WHERE b.category = a.category) ORDER BY a.id DESC;
