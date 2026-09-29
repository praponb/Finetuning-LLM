SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
    FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
    LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
    LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating
FROM suppliers;
