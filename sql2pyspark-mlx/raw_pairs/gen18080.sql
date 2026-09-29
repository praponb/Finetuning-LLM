SELECT subscriptions.id, subscriptions.end_date, subscriptions.status FROM subscriptions WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE city NOT IN ('London', 'New York'))
