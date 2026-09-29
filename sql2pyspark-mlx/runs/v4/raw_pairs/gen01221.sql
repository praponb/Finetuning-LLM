SELECT plan, MIN(end_date) AS first_end_date, MIN(start_date) AS first_start_date FROM subscriptions WHERE monthly_fee >= 100 GROUP BY plan ORDER BY first_end_date ASC;
