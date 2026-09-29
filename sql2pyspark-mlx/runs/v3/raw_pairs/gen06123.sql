SELECT start_date FROM subscriptions WHERE status <> 'ACTIVE' UNION SELECT opened_date FROM stores WHERE opened_date > '2024-09-01';
