WITH base AS (
    SELECT *
    FROM suppliers
    WHERE (country = 'UK' AND country IN ('US', 'TH')) OR city IN ('Tokyo', 'London')
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, AVG(rating) AS avg_rating, MAX(rating) AS max_rating
    FROM base
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, avg_rating, max_rating
FROM rating_bucket_summary
ORDER BY rating_bucket, max_rating;
