SELECT
    id,
    region,
    sqft,
    SUBSTRING(store_name, 1, 4) AS store_name_prefix
FROM stores
WHERE YEAR(opened_date) >= 2025 AND region = 'East' AND YEAR(opened_date) = 2024
ORDER BY region, id
LIMIT 5;
