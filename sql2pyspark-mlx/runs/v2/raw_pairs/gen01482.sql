SELECT
    id,
    city,
    region,
    sqft,
    FLOOR(sqft) AS sqft_whole
FROM stores
WHERE UPPER(TRIM(store_name)) = 'AIRPORT KIOSK'
ORDER BY 5 DESC, city;
