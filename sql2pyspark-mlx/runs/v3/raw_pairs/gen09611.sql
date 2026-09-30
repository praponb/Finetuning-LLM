SELECT
    id,
    country,
    COALESCE(credit_limit, 0) AS credit_limit_filled,
    CAST(credit_limit AS INT) AS credit_limit_int
FROM customers
WHERE age < 55 AND (age <> 35 OR age BETWEEN 51 AND 60)
ORDER BY country ASC, id ASC
LIMIT 50;
