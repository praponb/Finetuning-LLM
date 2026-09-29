SELECT id, sqft
FROM stores
WHERE opened_date >= '2025-06-01'
ORDER BY sqft DESC;
