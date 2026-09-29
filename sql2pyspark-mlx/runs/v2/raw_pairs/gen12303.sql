SELECT status, customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE plan != 'enterprise' AND start_date BETWEEN '2023-04-21' AND '2024-12-01' AND monthly_fee < 100
GROUP BY status, customer_id
HAVING stddev_monthly_fee > 77
ORDER BY customer_id
