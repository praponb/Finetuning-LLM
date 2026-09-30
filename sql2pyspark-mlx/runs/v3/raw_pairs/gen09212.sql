SELECT status, MIN(start_date) AS first_start_date, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions GROUP BY status ORDER BY first_start_date, 1;
