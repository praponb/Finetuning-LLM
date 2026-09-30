SELECT s.sqft, i.quantity FROM stores s LEFT OUTER JOIN inventory i ON s.id = i.store_id WHERE i.quantity <= 112 ORDER BY quantity;
