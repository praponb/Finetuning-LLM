SELECT segment, credit_limit_count, max_age
FROM (
    SELECT segment, COUNT(credit_limit) AS credit_limit_count, MAX(age) AS max_age
    FROM customers
    WHERE city NOT IN ('Berlin', 'Singapore', 'New York')
    GROUP BY segment
) AS t
WHERE max_age >= 48
ORDER BY segment
LIMIT 20;
