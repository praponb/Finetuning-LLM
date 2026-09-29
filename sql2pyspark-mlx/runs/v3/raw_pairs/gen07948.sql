SELECT
    subscriptions.id,
    subscriptions.end_date,
    subscriptions.status,
    customers.city,
    customers.name,
    customers.email
FROM subscriptions
JOIN customers ON subscriptions.customer_id = customers.id
WHERE customers.email LIKE '%@mail.co.th'
ORDER BY customers.name DESC;
