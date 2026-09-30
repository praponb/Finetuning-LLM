SELECT subscriptions.id, subscriptions.plan, subscriptions.customer_id FROM subscriptions WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE NOT is_active);
