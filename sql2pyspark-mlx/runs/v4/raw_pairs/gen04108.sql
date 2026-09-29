WITH Base AS (
    SELECT *
    FROM stores
    WHERE NOT (store_name LIKE 'Sia%' AND (opened_date >= '2023-08-01' OR sqft BETWEEN 8893 AND 10831))
),
agg AS (
    SELECT city, COUNT(*) AS num_stores, AVG(sqft) AS avg_sqft
    FROM Base
    GROUP BY city
)
SELECT city, num_stores, avg_sqft
FROM agg
WHERE num_stores > 7
ORDER BY num_stores DESC
