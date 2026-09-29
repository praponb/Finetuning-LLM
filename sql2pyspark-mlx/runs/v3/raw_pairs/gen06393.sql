SELECT QUARTER(start_date) AS start_quarter, MIN(start_date) AS first_start_date FROM subscriptions GROUP BY QUARTER(start_date) ORDER BY first_start_date ASC;
