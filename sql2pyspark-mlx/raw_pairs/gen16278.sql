SELECT p.category, MAX(oi.unit_price) AS max_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.category
HAVING MAX(oi.unit_price) > 210;
