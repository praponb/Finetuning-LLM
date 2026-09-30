SELECT
    *,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
    MIN(rating) OVER (PARTITION BY city, country) AS city_min_rating,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
FROM suppliers
