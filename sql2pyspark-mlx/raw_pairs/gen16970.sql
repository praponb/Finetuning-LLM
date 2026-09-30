WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country = 'DE'
),
agg AS (
    SELECT country, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(rating) AS rating_count
    FROM src
    GROUP BY country, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY country, rating_bucket
LIMIT 10;
