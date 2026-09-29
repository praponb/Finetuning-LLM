SELECT a.id, a.category, a.cost, a.product_name FROM products a WHERE a.cost = (SELECT MIN(b.cost) FROM products b WHERE b.category = a.category);
