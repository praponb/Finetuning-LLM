SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, MIN(paid_at) AS first_paid_at
FROM payments
WHERE amount < 610
GROUP BY DATE_TRUNC('MONTH', paid_at)
ORDER BY paid_month;
