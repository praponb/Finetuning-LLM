SELECT
    country,
    SUM(rating) AS total_rating,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk
FROM suppliers
GROUP BY country
ORDER BY rnk, cnt DESC
