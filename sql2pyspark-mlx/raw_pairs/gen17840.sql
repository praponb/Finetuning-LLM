SELECT
    id,
    sqft,
    opened_date,
    city
FROM stores
WHERE sqft >= 15865
ORDER BY sqft DESC;
