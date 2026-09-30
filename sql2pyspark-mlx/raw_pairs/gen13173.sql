SELECT
    MONTH(paid_at) AS paid_month,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(amount) AS total_amount
FROM payments
WHERE method IN ('card', 'promptpay')
GROUP BY MONTH(paid_at)
HAVING SUM(amount) < 5100
ORDER BY stddev_amount DESC;
