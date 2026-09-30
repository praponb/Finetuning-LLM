SELECT opened_date AS event_date FROM accounts WHERE opened_date IS NOT NULL UNION SELECT end_date AS event_date FROM subscriptions WHERE end_date IS NOT NULL ORDER BY event_date
