SELECT plan, MIN(start_date) AS min_start_date, COUNT(*) AS subscription_count, MAX(end_date) AS latest_end_date FROM subscriptions GROUP BY plan ORDER BY subscription_count ASC, min_start_date ASC
