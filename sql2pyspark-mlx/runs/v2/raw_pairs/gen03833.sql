SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM suppliers
GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
HAVING min_rating > 3.3
ORDER BY min_rating DESC, rating_bucket
LIMIT 10;
