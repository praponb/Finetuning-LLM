SELECT
    id,
    city,
    sqft,
    MAX(sqft) OVER (PARTITION BY city) AS city_max_sqft
FROM stores;
