SELECT status, latest_paid_at, total_amount
FROM (
    SELECT status, MAX(paid_at) AS latest_paid_at, SUM(amount) AS total_amount
    FROM payments
    GROUP BY status
) AS sub
WHERE total_amount >= 7300
ORDER BY status DESC
LIMIT 5;
