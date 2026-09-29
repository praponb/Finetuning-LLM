WITH filtered_customers AS (
    SELECT *, UPPER(city) AS city_upper
    FROM customers
    WHERE country != 'TH' AND MONTH(signup_date) IN (1, 5, 6)
),
Agg AS (
    SELECT
        city,
        is_active,
        SUM(CASE WHEN segment = 'Consumer' THEN age ELSE 0 END) AS consumer_age,
        MAX(age) AS max_age
    FROM filtered_customers
    GROUP BY city, is_active
)
SELECT
    city,
    is_active,
    consumer_age,
    max_age
FROM Agg;
