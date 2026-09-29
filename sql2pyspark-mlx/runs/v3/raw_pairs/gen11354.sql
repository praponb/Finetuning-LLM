SELECT a.id, a.city, a.sqft FROM stores a WHERE a.sqft = (SELECT MIN(b.sqft) FROM stores b WHERE b.city = a.city) ORDER BY a.id DESC;
