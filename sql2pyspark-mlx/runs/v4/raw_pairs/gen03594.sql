SELECT a.id, a.city, a.sqft, a.opened_date FROM stores a WHERE a.sqft < (SELECT AVG(b.sqft) FROM stores b WHERE b.city = a.city);
