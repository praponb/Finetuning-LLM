SELECT status, MIN(monthly_fee) AS min_monthly_fee, MAX(end_date) AS last_end_date, COUNT(*) AS row_count FROM subscriptions GROUP BY status HAVING MIN(monthly_fee) > 230 ORDER BY last_end_date;
