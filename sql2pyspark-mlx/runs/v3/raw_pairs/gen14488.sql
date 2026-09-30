SELECT *
FROM (
    SELECT TO_DATE(paid_at) paid_day, ROUND(AVG(amount), 2) avg_amount, MAX(amount) - MIN(amount) amount_range
    FROM payments
    GROUP BY TO_DATE(paid_at)
) sub
WHERE avg_amount > 43
ORDER BY paid_day
