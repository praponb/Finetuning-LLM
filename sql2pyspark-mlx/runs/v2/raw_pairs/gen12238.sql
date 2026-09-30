SELECT plan, MIN(end_date) AS min_end_date, AVG(monthly_fee) AS avg_monthly_fee FROM subscriptions GROUP BY plan ORDER BY plan, min_end_date;
