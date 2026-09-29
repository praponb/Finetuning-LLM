SELECT s.country, SUM(oi.quantity * oi.unit_price) revenue
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country IN ('UK', 'JP')
GROUP BY s.country
HAVING SUM(oi.quantity * oi.unit_price) >= 29
ORDER BY revenue DESC;
