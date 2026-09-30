WITH Ranked AS (
    SELECT
        id,
        country,
        credit_limit,
        city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rn
    FROM customers
    WHERE LOWER(TRIM(email)) = 'liam@outlook.com'
)
SELECT
    id,
    country,
    credit_limit,
    city
FROM Ranked
WHERE rn <= 3;
