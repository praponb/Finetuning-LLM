SELECT MONTH(start_date) AS start_month, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count FROM subscriptions GROUP BY 1;
