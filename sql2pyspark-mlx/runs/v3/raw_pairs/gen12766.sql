SELECT
    id,
    store_name,
    region,
    sqft
FROM stores
WHERE YEAR(opened_date) >= 2024;
