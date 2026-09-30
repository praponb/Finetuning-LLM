SELECT sqft
FROM stores
WHERE LOWER(TRIM(store_name)) = 'siam flagship' AND (region IN ('Central', 'East', 'South') OR sqft < 8785)
ORDER BY sqft
