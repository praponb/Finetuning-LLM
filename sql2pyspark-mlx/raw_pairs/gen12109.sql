SELECT
    id,
    rating,
    MAX(rating) OVER w_part AS last4_max_rating,
    COUNT(rating) OVER w_part AS last4_count_rating,
    AVG(rating) OVER w_part AS last4_avg_rating
FROM suppliers
WINDOW w_part AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
