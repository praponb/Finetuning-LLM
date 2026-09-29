SELECT
    id,
    name,
    email,
    country,
    signup_date,
    credit_limit,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date DESC NULLS LAST, id ASC) AS rank_in_group
    FROM customers
) x
WHERE rank_in_group = 1;
