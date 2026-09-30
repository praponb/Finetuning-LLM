SELECT method, max_amount
FROM (
    SELECT method, MAX(amount) AS max_amount
    FROM payments
    GROUP BY method
) AS x
WHERE max_amount > 1000
ORDER BY max_amount DESC, method
LIMIT 5;
