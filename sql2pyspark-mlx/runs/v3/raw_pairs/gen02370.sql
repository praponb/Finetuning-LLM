SELECT p.id, p.price, s.country, s.rating, p.price + p.cost AS margin FROM products p JOIN suppliers s ON s.id = p.supplier_id WHERE p.cost > 330;
