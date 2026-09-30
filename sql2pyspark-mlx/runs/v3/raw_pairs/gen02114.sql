SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, country, SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) AS de_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END, country
HAVING de_rating >= 27;
