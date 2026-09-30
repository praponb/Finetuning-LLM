SELECT s.sqft, o.store_id, LOWER(s.store_name) store_name_lower
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id;
