SELECT *
FROM (
    SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END rating_bucket, ROUND(AVG(rating), 2) avg_rating, SUM(CASE WHEN country = 'TH' THEN rating ELSE 0 END) th_rating
    FROM suppliers
    WHERE country = 'US'
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
) t
WHERE avg_rating > 27
ORDER BY rating_bucket, avg_rating DESC;
