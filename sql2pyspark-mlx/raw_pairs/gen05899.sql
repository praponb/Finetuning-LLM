WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM customers
    EXCEPT
    SELECT id, city
    FROM customers
    WHERE is_active
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age
FROM reconciled
ORDER BY city DESC;
