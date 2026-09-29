SELECT
    id,
    rating,
    city,
    LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating,
    LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
FROM suppliers
