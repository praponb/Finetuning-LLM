SELECT QUARTER(c.signup_date) AS signup_quarter, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range
FROM subscriptions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.monthly_fee <= 170
GROUP BY QUARTER(c.signup_date)
ORDER BY signup_quarter DESC
