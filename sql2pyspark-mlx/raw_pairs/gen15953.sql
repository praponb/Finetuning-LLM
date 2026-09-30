SELECT
    id,
    opened_date,
    sqft,
    store_name
FROM stores
WHERE opened_date = (SELECT MIN(opened_date) FROM stores);
