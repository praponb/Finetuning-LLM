SELECT
    opened_date,
    sqft,
    city,
    UPPER(region) AS region_upper
FROM stores
WHERE sqft BETWEEN 10495 AND 16076;
