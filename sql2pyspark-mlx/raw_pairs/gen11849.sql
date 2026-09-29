WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date NULLS FIRST, id) AS rank_in_group
    FROM customers
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    rank_in_group
FROM latest
ORDER BY rank_in_group DESC
