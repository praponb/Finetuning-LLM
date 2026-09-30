SELECT plan, MAX(start_date) AS latest_start_date FROM subscriptions GROUP BY plan ORDER BY plan ASC
