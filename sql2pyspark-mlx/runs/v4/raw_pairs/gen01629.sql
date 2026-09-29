WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE (rating BETWEEN 1.9 AND 4.1 AND LOWER(TRIM(supplier_name)) = 'global supply') OR city IN ('London', 'Berlin', 'Tokyo')
),
rating_bucket_summary AS (
    SELECT
        CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket,
        city,
        MIN(rating) AS min_rating,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list
    FROM filtered_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END, city
)
SELECT *
FROM rating_bucket_summary
WHERE min_rating > 3.1
ORDER BY rating_bucket
