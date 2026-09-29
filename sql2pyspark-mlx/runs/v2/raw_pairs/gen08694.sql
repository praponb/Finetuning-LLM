SELECT s.city, p.stock_qty, p.product_name FROM suppliers s INNER JOIN products p ON s.id = p.supplier_id WHERE s.rating < 3.5 OR p.cost >= 510 ORDER BY product_name, s.city;
