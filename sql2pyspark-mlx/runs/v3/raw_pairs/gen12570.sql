SELECT TO_DATE(paid_at) AS paid_day, ROUND(STDDEV(amount), 2) AS stddev_amount, MIN(amount) AS min_amount
FROM payments
GROUP BY TO_DATE(paid_at)
