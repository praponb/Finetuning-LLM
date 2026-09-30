WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating <= 2.8
),
summary AS (
    SELECT city, CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM filtered_suppliers
    GROUP BY city, CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY stddev_rating DESC, city ASC, rating_bucket ASC
LIMIT 20
