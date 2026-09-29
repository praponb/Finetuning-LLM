SELECT country, MIN(rating) AS min_rating, ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th
FROM suppliers
GROUP BY country
HAVING COUNT(CASE WHEN country = 'SG' THEN 1 END) < 2;
