SELECT a.id, a.city, a.sqft FROM stores a WHERE a.sqft < (SELECT AVG(b.sqft) FROM stores b WHERE b.city = a.city) ORDER BY a.id DESC, a.sqft DESC;
