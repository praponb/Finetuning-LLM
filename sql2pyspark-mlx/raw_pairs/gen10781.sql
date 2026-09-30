SELECT
    id,
    city,
    rating,
    LAST_VALUE(rating, TRUE) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
    AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior
FROM suppliers;
