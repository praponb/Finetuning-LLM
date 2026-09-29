SELECT
    credit_limit,
    city,
    name,
    TRIM(country) clean_country
FROM customers
WHERE age BETWEEN 38 AND 65
