SELECT
    id,
    sqft,
    city,
    store_name
FROM stores
WHERE opened_date < '2023-03-01'
ORDER BY store_name ASC;
