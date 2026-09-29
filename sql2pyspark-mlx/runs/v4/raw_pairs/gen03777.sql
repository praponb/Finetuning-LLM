SELECT a.id, a.category, a.price, a.supplier_id FROM products a WHERE a.price > (SELECT AVG(b.price) FROM products b WHERE b.category = a.category);
