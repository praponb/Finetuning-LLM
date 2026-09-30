SELECT
    id,
    sqft,
    opened_date,
    store_name
FROM stores
WHERE sqft >= (SELECT AVG(sqft) FROM stores)
