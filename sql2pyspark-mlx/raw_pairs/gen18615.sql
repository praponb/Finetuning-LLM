SELECT
    id,
    country,
    rating,
    MAX(rating) OVER w_recent AS last3_max_rating,
    COUNT(rating) OVER w_recent AS last3_count_rating,
    MIN(rating) OVER w_recent AS last3_min_rating
FROM suppliers
WHERE country IN ('UK', 'JP', 'SG')
WINDOW w_recent AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
