SELECT
    id,
    rating,
    country,
    SUM(rating) OVER (ORDER BY rating, id) AS running_rating,
    AVG(rating) OVER (PARTITION BY country) AS country_avg_rating
FROM suppliers
WHERE supplier_name IS NOT NULL;
