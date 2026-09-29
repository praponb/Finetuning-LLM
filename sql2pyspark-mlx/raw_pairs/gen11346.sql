WITH src AS (
    SELECT *, COALESCE(rating, 0) AS rating_filled
    FROM suppliers
    WHERE rating BETWEEN 2.9 AND 3.1
),
summary AS (
    SELECT CASE WHEN rating_filled >= 73 THEN 'high' WHEN rating_filled >= 31 THEN 'mid' ELSE 'low' END AS rating_filled_bucket, city, MIN(rating_filled) AS min_rating_filled
    FROM src
    GROUP BY CASE WHEN rating_filled >= 73 THEN 'high' WHEN rating_filled >= 31 THEN 'mid' ELSE 'low' END, city
)
SELECT rating_filled_bucket, min_rating_filled
FROM summary
WHERE min_rating_filled < 71
ORDER BY min_rating_filled DESC, rating_filled_bucket;
