SELECT store_name, UPPER(region) AS region_upper
FROM stores
WHERE sqft > 8527 AND sqft = 10720
ORDER BY region_upper
