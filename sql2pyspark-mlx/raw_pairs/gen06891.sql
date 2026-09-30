SELECT s.country, SUM(oi.quantity * oi.unit_price) AS revenue FROM order_items oi JOIN products p ON p.id = oi.product_id RIGHT JOIN suppliers s ON s.id = p.supplier_id GROUP BY s.country;
