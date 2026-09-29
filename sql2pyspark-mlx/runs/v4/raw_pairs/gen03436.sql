SELECT
    id,
    rating,
    country,
    ROUND(AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rating_moving_avg_7,
    FIRST_VALUE(country) OVER (ORDER BY rating DESC, id) AS first_country
FROM suppliers
WHERE rating < 2.0
