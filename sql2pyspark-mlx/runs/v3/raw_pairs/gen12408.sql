SELECT
    customers.id,
    customers.age,
    customers.segment,
    customers.is_active
FROM customers
WHERE EXISTS (SELECT 1 FROM subscriptions WHERE subscriptions.customer_id = customers.id AND MONTH(subscriptions.end_date) IN (4, 6, 11));
