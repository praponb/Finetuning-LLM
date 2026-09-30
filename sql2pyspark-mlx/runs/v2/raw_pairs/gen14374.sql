WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('JP', 'DE') AND (city IN ('New York', 'Berlin', 'Bangkok') OR supplier_name LIKE 'Gl%')
),
agg AS (
    SELECT CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) AS max_rating
    FROM src
    GROUP BY CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, max_rating
FROM agg
ORDER BY rating_bucket, 2 DESC
LIMIT 1;
