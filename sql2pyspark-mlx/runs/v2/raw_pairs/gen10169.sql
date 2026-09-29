WITH base AS (
    SELECT *
    FROM suppliers
    WHERE country = 'DE' AND (city != 'London' OR supplier_name LIKE '%Suppl%')
),
summary AS (
    SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND(STDDEV(rating), 2) AS stddev_rating, SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating
    FROM base
    GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY jp_rating DESC;
