select customers.segment, customers.is_active, subscriptions.customer_id from customers left outer join subscriptions on subscriptions.customer_id = customers.id order by segment;
