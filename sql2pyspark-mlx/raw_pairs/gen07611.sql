SELECT i.id, i.store_id, s.region, s.sqft, p.product_name, p.cost FROM inventory i LEFT JOIN stores s ON s.id = i.store_id FULL OUTER JOIN products p ON i.product_id = p.id ORDER BY 3 DESC;
