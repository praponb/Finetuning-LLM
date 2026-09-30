SELECT *, LOWER(region) region_lower
FROM stores
WHERE sqft BETWEEN 6049 AND 10437 OR region IN ('Central', 'East')
ORDER BY store_name ASC, id ASC
LIMIT 10
