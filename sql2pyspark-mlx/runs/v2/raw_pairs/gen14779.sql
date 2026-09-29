SELECT city, ROUND((100 * SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sg, ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
WHERE NOT (supplier_name LIKE '%Gmb%' AND city IN ('London', 'Chiang Mai', 'Singapore') AND city NOT IN ('Singapore', 'Chiang Mai'))
GROUP BY city
ORDER BY city
