SELECT i.id, i.product_id, p.price, s.rating, s.country FROM inventory i JOIN products p ON i.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id;
