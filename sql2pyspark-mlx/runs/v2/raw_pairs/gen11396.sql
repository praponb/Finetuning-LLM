WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city IS NULL
),
agg AS (
    SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, COUNT(*) AS supplier_count
    FROM filtered_suppliers
    GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY rating_bucket;
