WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name IS NOT NULL AND city = 'Berlin'
),
suppliers_stats AS (
    SELECT
        CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
        country,
        COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM recent_suppliers
    GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END, country
)
SELECT *
FROM suppliers_stats
WHERE avg_rating > 57
ORDER BY avg_rating DESC, rating_bucket
