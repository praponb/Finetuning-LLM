SELECT oi.product_id, p.cost, s.city, s.country FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY city, oi.product_id DESC
