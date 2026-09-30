SELECT
    country,
    COUNT(DISTINCT rating) AS unique_citys,
    COUNT(rating) AS rating_count,
    MAX(rating) AS max_rating
FROM suppliers
WHERE country NOT IN ('DE', 'US', 'TH')
GROUP BY country
HAVING unique_citys >= 1;
