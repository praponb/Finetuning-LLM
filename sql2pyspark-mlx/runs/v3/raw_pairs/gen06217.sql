WITH Base AS (
    SELECT *, CONCAT_WS('-', city, supplier_name) AS city_supplier_name_key
    FROM suppliers
    WHERE country = 'JP'
),
Summary AS (
    SELECT city, CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
    FROM Base
    GROUP BY city, CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END
)
SELECT city, rating_bucket
FROM Summary
