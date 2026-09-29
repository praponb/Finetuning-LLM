SELECT
    id,
    city,
    sqft,
    region
FROM stores
WHERE region IS NOT NULL;
