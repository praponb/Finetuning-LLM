SELECT p.product_name, p.supplier_id, s.supplier_name, s.city, s.rating, FLOOR(p.price) AS price_whole FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id;
