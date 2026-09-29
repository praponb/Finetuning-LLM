SELECT
    country,
    city,
    ROUND(AVG(rating), 2) avg_rating,
    SUM(rating) total_rating
FROM suppliers
WHERE rating < 3.2 AND (supplier_name LIKE '%Wholesa%' OR country = 'US')
GROUP BY 1, 2
