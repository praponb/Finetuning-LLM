SELECT
    id,
    country,
    rating,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id) AS running_rating,
    LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating
FROM suppliers
WHERE country != 'TH';
