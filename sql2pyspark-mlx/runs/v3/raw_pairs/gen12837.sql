SELECT i.quantity, p.launch_date, p.category, s.rating, LOWER(s.city) AS city_lower FROM inventory i RIGHT JOIN products p ON i.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id;
