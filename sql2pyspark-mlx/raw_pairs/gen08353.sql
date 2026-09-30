SELECT id, country, signup_date
FROM (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY signup_date DESC) AS rn
    FROM customers
) AS r
WHERE rn <= 5
