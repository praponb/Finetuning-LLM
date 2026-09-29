SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
ORDER BY paid_month DESC;
