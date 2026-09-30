SELECT
    id,
    rating,
    city,
    LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS dense_rnk
FROM suppliers
ORDER BY dense_rnk DESC
