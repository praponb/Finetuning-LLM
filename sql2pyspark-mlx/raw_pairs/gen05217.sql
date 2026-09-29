SELECT s.sqft, s.city, i.last_restock FROM stores s LEFT OUTER JOIN inventory i ON i.store_id = s.id ORDER BY sqft;
