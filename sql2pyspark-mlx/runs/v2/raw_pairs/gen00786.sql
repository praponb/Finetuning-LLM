SELECT
    YEAR(paid_at) AS paid_year,
    method,
    COUNT(amount) AS amount_count,
    SUM(amount) AS total_amount,
    COUNT(*) AS num_payments
FROM payments
GROUP BY YEAR(paid_at), method
HAVING total_amount > 2600
ORDER BY method DESC;
