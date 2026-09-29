WITH base AS (
    SELECT *, CONCAT(country, ' - ', city) country_city
    FROM suppliers
    WHERE supplier_name IS NOT NULL AND rating BETWEEN 2.0 AND 3.2
),
agg AS (
    SELECT city, COUNT(DISTINCT rating) unique_citys, COUNT(CASE WHEN country = 'DE' THEN 1 END) de_count
    FROM base
    GROUP BY city
)
SELECT *
FROM agg
WHERE de_count > 5;
