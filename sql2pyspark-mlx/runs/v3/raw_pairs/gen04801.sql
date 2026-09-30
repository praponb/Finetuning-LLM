SELECT city, rating_bucket, berlin_rating
FROM (
    SELECT city, CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END rating_bucket, SUM(CASE WHEN city = 'Berlin' THEN rating ELSE 0 END) berlin_rating
    FROM suppliers
    GROUP BY city, CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END
) t
WHERE berlin_rating < 61;
