SELECT
    id,
    opened_date,
    FLOOR(sqft) AS sqft_whole,
    LENGTH(city) AS city_length
FROM stores
WHERE city = 'Bangkok';
