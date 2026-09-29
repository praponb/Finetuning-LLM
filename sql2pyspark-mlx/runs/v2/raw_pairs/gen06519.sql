SELECT TO_DATE(paid_at) AS paid_day, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE NOT (amount BETWEEN 550 AND 670 AND amount < 1240)
GROUP BY TO_DATE(paid_at)
ORDER BY stddev_amount DESC
