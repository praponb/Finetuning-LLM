SELECT
    city,
    segment,
    max_credit_limit,
    avg_age
FROM (
    SELECT
        city,
        segment,
        MAX(credit_limit) AS max_credit_limit,
        ROUND(AVG(age), 2) AS avg_age
    FROM customers
    WHERE age BETWEEN 50 AND 65
    GROUP BY city, segment
) AS x
WHERE avg_age < 70
ORDER BY max_credit_limit DESC;
