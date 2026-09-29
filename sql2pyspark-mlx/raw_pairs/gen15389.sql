WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) = 1
)
SELECT
    id,
    name,
    email,
    country,
    city,
    signup_date,
    credit_limit,
    age
FROM deduped;
