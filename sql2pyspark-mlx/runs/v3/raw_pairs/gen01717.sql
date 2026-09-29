SELECT opened_date AS event_date FROM stores UNION ALL SELECT end_date AS event_date FROM subscriptions ORDER BY event_date;
