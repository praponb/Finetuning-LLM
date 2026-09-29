SELECT paid_hour, avg_amount
FROM (
    SELECT HOUR(paid_at) AS paid_hour, AVG(amount) AS avg_amount
    FROM payments
    GROUP BY HOUR(paid_at)
) AS agg
WHERE avg_amount > 1450
ORDER BY avg_amount DESC, paid_hour DESC
LIMIT 1;
