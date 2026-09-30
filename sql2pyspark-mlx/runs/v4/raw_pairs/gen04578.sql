WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END AS is_corporate,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-10-06'
)
SELECT city, segment
FROM filtered
INTERSECT
SELECT city, segment
FROM customers
WHERE name LIKE '%Hen%';
