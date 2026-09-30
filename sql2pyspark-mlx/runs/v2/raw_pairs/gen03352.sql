SELECT customer_id, status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee FROM subscriptions GROUP BY CUBE(customer_id, status) ORDER BY status;
