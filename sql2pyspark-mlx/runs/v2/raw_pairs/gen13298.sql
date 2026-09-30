SELECT i.store_id, p.launch_date, s.sqft
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id;
