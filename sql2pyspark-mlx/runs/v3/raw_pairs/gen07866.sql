SELECT
    id,
    region,
    sqft,
    TRIM(city) AS clean_city
FROM stores
WHERE UPPER(TRIM(store_name)) = 'AIRPORT KIOSK' OR store_name LIKE 'M%'
ORDER BY id, sqft
