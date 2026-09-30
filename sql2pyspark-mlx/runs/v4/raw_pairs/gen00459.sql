SELECT
    id,
    segment,
    country,
    age,
    credit_limit,
    FIRST_VALUE(credit_limit) OVER (PARTITION BY segment, country ORDER BY age, id) AS first_credit_limit
FROM customers
WHERE country NOT IN ('TH', 'SG')
ORDER BY credit_limit DESC
