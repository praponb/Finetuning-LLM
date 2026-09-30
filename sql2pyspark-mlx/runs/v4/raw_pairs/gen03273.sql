SELECT id, sqft, store_name
FROM stores
WHERE region NOT IN ('Central', 'West', 'South')
EXCEPT
SELECT id, sqft, store_name
FROM stores
WHERE UPPER(store_name) LIKE '%IVE%'
