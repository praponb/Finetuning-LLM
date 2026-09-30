SELECT
    id,
    store_name,
    city,
    REGEXP_REPLACE(store_name, ' ', '_') AS store_name_slug
FROM stores
WHERE (store_name LIKE 'Dow%' AND sqft < 9960) OR opened_date < '2023-08-04'
ORDER BY city, id
LIMIT 3
