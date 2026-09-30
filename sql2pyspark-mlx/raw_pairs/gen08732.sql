WITH remaining_keys AS (
    SELECT country
    FROM customers
    INTERSECT
    SELECT country
    FROM customers
    WHERE credit_limit >= 9600
),
reconciled AS (
    SELECT s.*
    FROM customers s
    INNER JOIN remaining_keys k ON s.country = k.country
)
SELECT
    id,
    email,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    age
FROM reconciled
ORDER BY city DESC NULLS LAST, id;
