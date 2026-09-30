SELECT *
FROM (
    SELECT is_active, AVG(credit_limit) AS avg_credit_limit
    FROM customers
    WHERE credit_limit > 10800
    GROUP BY is_active
) AS s
WHERE avg_credit_limit >= 9100
ORDER BY avg_credit_limit ASC;
