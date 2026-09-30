SELECT
    city,
    country,
    MAX(rating) - MIN(rating) rating_range,
    MIN(rating) min_rating
FROM suppliers
GROUP BY 1, 2;
