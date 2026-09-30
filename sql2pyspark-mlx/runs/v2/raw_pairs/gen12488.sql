SELECT s.id, s.sqft
FROM stores s
WHERE s.id IN (SELECT store_id FROM store_sales)
ORDER BY id DESC, s.sqft DESC;
