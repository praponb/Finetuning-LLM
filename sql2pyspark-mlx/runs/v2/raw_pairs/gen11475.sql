SELECT s.city, i.store_id, i.last_restock, i.product_id FROM stores s LEFT OUTER JOIN inventory i ON i.store_id = s.id WHERE s.sqft IS NULL ORDER BY store_id DESC;
