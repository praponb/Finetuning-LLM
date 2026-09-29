SELECT
    ss.units,
    s.sqft,
    s.region,
    s.city
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE s.city = 'Berlin'
ORDER BY city DESC;
