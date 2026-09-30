SELECT paid_month, num_payments
FROM (
    SELECT DATE_FORMAT(paid_at, 'yyyy-MM') paid_month, COUNT(*) num_payments
    FROM payments
    WHERE amount > 1300
    GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
) t
WHERE num_payments > 4
ORDER BY paid_month, num_payments
