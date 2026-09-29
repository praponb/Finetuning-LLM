WITH ValidSuppliers AS (
    SELECT *
    FROM suppliers
    WHERE country <> 'JP' AND city IN ('Berlin', 'New York') AND city IN ('Chiang Mai', 'Berlin', 'Singapore')
),
agg AS (
    SELECT country, MAX(rating) - MIN(rating) AS rating_range, COUNT(DISTINCT rating) AS unique_citys
    FROM ValidSuppliers
    GROUP BY country
)
SELECT country, rating_range, unique_citys
FROM agg
ORDER BY rating_range DESC;
