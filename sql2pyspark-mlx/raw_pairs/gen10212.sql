SELECT
    id,
    city,
    rating,
    SUM(rating) OVER (PARTITION BY city) AS city_sum_rating
FROM suppliers;
