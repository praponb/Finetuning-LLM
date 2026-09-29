SELECT method, amount_range, total_amount
FROM (
    SELECT method, MAX(amount) - MIN(amount) amount_range, SUM(amount) total_amount
    FROM payments
    GROUP BY method
) sub
WHERE amount_range >= 47
ORDER BY amount_range DESC
