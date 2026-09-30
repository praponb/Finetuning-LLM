SELECT
    id,
    store_name,
    city,
    opened_date,
    CASE WHEN region = 'South' THEN 1 ELSE 0 END is_south,
    ROUND(sqft * 1.07, 2) adjusted_sqft
FROM stores
WHERE sqft IS NOT NULL AND (opened_date > '2023-09-01' OR store_name NOT LIKE 'Mal%')
ORDER BY store_name, adjusted_sqft
