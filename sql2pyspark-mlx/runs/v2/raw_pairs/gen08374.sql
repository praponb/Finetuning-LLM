SELECT p.supplier_id, SUM(oi.quantity) AS total_quantity FROM order_items oi INNER JOIN products p ON oi.product_id = p.id WHERE oi.quantity > 6 GROUP BY p.supplier_id;
