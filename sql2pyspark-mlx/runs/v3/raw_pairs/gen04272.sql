SELECT s.city, p.price, p.product_name, p.cost FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id ORDER BY cost DESC;
