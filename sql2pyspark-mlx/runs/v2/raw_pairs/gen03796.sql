SELECT country, SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) AS de_rating, ROUND((100 * SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_us
FROM suppliers
WHERE country <> 'TH'
GROUP BY country;
