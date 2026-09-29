SELECT
    id,
    country,
    city,
    age,
    SUM(age) OVER (PARTITION BY country, city) country_sum_age
FROM customers
WHERE credit_limit < 15600;
