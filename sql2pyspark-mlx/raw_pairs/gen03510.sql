SELECT id, sqft, region
FROM stores
WHERE MONTH(opened_date) IN (1, 7, 8)
EXCEPT
SELECT id, sqft, region
FROM stores
WHERE store_name NOT LIKE '%Flagship%'
