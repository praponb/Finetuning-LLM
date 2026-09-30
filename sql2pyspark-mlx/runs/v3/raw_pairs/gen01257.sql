SELECT *
FROM (
    SELECT
        id,
        country,
        age,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS pos
    FROM customers
) AS t
WHERE pos = 1;
