SELECT SUM(sqft) AS total_sqft
FROM stores
WHERE store_name NOT LIKE 'Ol%'
