WITH valid_suppliers AS (
    SELECT *, UPPER(city) AS city_upper
    FROM suppliers
    WHERE country IN ('TH', 'UK', 'US') OR city = 'Chiang Mai'
),
suppliers_stats AS (
    SELECT city, COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
    FROM valid_suppliers
    GROUP BY city
)
SELECT *
FROM suppliers_stats
ORDER BY us_count DESC;
