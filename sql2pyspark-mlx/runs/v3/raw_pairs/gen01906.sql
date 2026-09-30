SELECT
    id,
    sqft,
    opened_date,
    region,
    COALESCE(store_name, 'Unknown') AS store_name_filled
FROM stores
WHERE YEAR(opened_date) = 2025 AND store_name LIKE 'Sia%'
ORDER BY region;
