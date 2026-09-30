SELECT
    id,
    city,
    opened_date,
    sqft,
    CONCAT(region, '/', store_name) AS region_store_name
FROM stores
WHERE region IS NULL AND region NOT IN ('South', 'Central')
ORDER BY id DESC;
