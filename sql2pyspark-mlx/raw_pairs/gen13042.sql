SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM suppliers
WHERE rating > 3.0 AND country = 'TH'
GROUP BY 1
ORDER BY min_rating, rating_bucket;
