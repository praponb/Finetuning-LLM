SELECT *
FROM (
    SELECT city, country, MAX(credit_limit) AS max_credit_limit
    FROM customers
    GROUP BY city, country
) AS s
WHERE max_credit_limit < 15100;
