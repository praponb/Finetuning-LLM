SELECT method, unique_orders, unique_statuss
FROM (
    SELECT method, COUNT(DISTINCT amount) AS unique_orders, COUNT(DISTINCT amount) AS unique_statuss
    FROM payments
    WHERE status IN ('REFUNDED', 'FAILED')
    GROUP BY method
) AS s
WHERE unique_orders < 9
ORDER BY method
