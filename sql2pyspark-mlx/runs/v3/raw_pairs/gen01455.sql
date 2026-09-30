SELECT
    id,
    store_name,
    city,
    opened_date
FROM stores
WHERE city <> 'Singapore'
