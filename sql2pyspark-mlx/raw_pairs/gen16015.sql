SELECT plan, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE monthly_fee > 200 OR start_date > '2023-09-01'
GROUP BY plan
HAVING ROUND(STDDEV(monthly_fee), 2) >= 42;
