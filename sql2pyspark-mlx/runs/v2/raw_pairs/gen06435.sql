SELECT id, opened_date, region
FROM stores
WHERE region IN ('Central', 'West')
ORDER BY 2, 1
LIMIT 5
