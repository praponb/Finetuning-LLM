SELECT
    method,
    SUM(amount) total_amount,
    COUNT(*) cnt,
    ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM payments
WHERE method != 'card'
GROUP BY method
ORDER BY pct_of_total
