SELECT id, country, signup_date
FROM (
    SELECT
        id,
        country,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS row_num
    FROM customers
) AS r
WHERE row_num <= 3;
