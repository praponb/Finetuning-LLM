SELECT customers.segment, subscriptions.monthly_fee FROM customers LEFT OUTER JOIN subscriptions ON customers.id = subscriptions.customer_id;
