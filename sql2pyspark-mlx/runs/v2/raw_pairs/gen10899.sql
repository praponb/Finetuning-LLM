SELECT
    id,
    region,
    LENGTH(store_name) AS store_name_length,
    UPPER(region) AS region_upper
FROM stores
WHERE city IN ('Singapore', 'Bangkok', 'Chiang Mai')
ORDER BY region ASC, region_upper ASC;
