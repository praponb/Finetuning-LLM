SELECT oi.unit_price, p.launch_date, s.city
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price > 420;
