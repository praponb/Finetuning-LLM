SELECT s.country, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, SUM(i.quantity) AS total_quantity
FROM inventory i
RIGHT OUTER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country;
