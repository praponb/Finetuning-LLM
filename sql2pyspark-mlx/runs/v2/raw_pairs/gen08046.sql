SELECT p.brand, COUNT(DISTINCT oi.quantity) AS unique_orders, SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY p.brand
ORDER BY revenue DESC
