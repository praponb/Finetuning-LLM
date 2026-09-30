SELECT
    id,
    age,
    email,
    segment,
    ROUND(credit_limit, 0) AS credit_limit_rounded
FROM customers
WHERE LOWER(name) LIKE '%ob %' AND city IS NULL
ORDER BY email, credit_limit_rounded DESC, id
LIMIT 10;
