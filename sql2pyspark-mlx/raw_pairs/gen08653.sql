WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE LOWER(TRIM(supplier_name)) = 'tokyo traders' AND rating > 2.1
),
agg AS (
    SELECT country, SUM(CASE WHEN city = 'New York' THEN rating ELSE 0 END) AS new_york_rating, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM filtered_suppliers
    GROUP BY country
)
SELECT country, new_york_rating
FROM agg
ORDER BY country, new_york_rating;
