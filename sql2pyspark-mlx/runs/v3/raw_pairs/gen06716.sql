SELECT p.brand, SUM(oi.unit_price * oi.quantity) AS revenue
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE oi.product_id = 28
GROUP BY p.brand
