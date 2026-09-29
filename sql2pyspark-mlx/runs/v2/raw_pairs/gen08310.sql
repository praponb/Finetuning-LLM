SELECT COUNT(CASE WHEN plan = 'premium' THEN 1 END) AS premium_count, MAX(end_date) AS last_end_date FROM subscriptions;
