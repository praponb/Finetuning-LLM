SELECT customers.id, customers.country, COUNT(subscriptions.id) AS subscription_count
FROM customers
LEFT JOIN subscriptions ON customers.id = subscriptions.customer_id
WHERE subscriptions.end_date > '2024-12-17'
GROUP BY customers.id, customers.country
ORDER BY customers.id DESC;
