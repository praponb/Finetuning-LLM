SELECT s.rating, p.price FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id ORDER BY s.rating DESC, price DESC;
