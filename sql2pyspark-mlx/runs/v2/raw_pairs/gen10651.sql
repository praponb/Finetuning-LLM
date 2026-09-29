SELECT plan, MAX(end_date) AS latest_end_date, COUNT(*) AS row_count FROM subscriptions GROUP BY plan;
