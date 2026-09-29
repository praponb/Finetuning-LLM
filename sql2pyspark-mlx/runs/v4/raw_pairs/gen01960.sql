SELECT oi.quantity, oi.unit_price, p.brand, s.country, s.rating FROM order_items oi RIGHT JOIN products p ON oi.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id;
