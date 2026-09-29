SELECT
    country,
    SUM(rating) AS total_rating,
    COUNT(*) AS cnt,
    SUM(SUM(rating)) OVER (ORDER BY country) AS cumulative_rating
FROM suppliers
GROUP BY country
ORDER BY country DESC;
