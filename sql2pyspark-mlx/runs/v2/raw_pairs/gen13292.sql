SELECT *
FROM (
    SELECT city, MAX(credit_limit) AS max_credit_limit
    FROM customers
    GROUP BY city
) AS sub
WHERE max_credit_limit >= 7700
ORDER BY city DESC;
