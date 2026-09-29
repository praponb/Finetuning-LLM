SELECT subscriptions.start_date, customers.email FROM subscriptions LEFT OUTER JOIN customers ON subscriptions.customer_id = customers.id;
