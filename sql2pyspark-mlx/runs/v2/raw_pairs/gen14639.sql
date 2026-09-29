SELECT
    id,
    country,
    age,
    city,
    ROUND(credit_limit * 0.85, 2) credit_limit_adjusted,
    LOWER(city) city_lower
FROM customers
WHERE age <> 61 AND email LIKE '%@company.co'
