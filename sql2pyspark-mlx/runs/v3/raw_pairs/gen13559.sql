SELECT plan, AVG(monthly_fee) AS avg_monthly_fee, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 180 AND monthly_fee <= 220 AND start_date BETWEEN '2024-02-01' AND '2025-04-14'
GROUP BY 1
HAVING stddev_monthly_fee < 40
