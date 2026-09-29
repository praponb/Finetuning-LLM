SELECT
    country,
    city,
    berlin_credit_limit,
    avg_age
FROM (
    SELECT
        country,
        city,
        SUM(CASE WHEN city = 'Berlin' THEN credit_limit ELSE 0 END) berlin_credit_limit,
        ROUND(AVG(age), 2) avg_age
    FROM customers
    GROUP BY country, city
) s
WHERE berlin_credit_limit > 49
