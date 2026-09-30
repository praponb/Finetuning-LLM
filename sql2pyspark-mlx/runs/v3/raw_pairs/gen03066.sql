SELECT s.id, s.city, s.sqft FROM stores s WHERE s.sqft = (SELECT MAX(s2.sqft) FROM stores s2 WHERE s2.city = s.city) ORDER BY s.id DESC;
