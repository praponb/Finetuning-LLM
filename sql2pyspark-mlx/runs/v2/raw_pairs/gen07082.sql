SELECT
    id,
    name,
    credit_limit,
    city,
    LOWER(country) AS country_lower
FROM customers
WHERE credit_limit > 12400 AND credit_limit > 10600
ORDER BY id DESC;
