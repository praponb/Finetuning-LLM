WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT country
    FROM customers
    INTERSECT
    SELECT country
    FROM customers
    WHERE segment != 'Consumer'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN remaining_keys k ON s.country = k.country
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    is_active
FROM reconciled
ORDER BY id DESC NULLS FIRST, city DESC;
