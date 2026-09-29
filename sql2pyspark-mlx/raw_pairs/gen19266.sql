SELECT
    id,
    city,
    age,
    country,
    LAST_VALUE(country) OVER (PARTITION BY city ORDER BY age DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_country
FROM customers;
