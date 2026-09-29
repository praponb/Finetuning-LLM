SELECT *
FROM stores
WHERE sqft IS NOT NULL AND store_name NOT LIKE '%Kio%' AND region IN ('East', 'South');
