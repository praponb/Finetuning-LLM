SELECT subscriptions.id, subscriptions.status FROM subscriptions WHERE subscriptions.customer_id NOT IN (SELECT id FROM customers WHERE country IN ('UK', 'US', 'SG'));
