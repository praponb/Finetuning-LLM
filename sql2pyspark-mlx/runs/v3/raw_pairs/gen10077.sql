SELECT
    DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY DATE_FORMAT(paid_at, 'yyyy-MM')) AS cumulative_amount
FROM payments
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
ORDER BY cnt DESC
