SELECT oi.id, oi.quantity, oi.product_id, p.price, p.brand, p.cost FROM order_items oi INNER JOIN products p ON oi.product_id = p.id ORDER BY price;
