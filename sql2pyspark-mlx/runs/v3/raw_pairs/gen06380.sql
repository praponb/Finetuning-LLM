SELECT country, segment, credit_limit_range
FROM (
    SELECT country, segment, MAX(credit_limit) - MIN(credit_limit) AS credit_limit_range
    FROM customers
    WHERE email LIKE '%@outlook.com'
    GROUP BY country, segment
) AS agg
WHERE credit_limit_range < 80
ORDER BY credit_limit_range;
