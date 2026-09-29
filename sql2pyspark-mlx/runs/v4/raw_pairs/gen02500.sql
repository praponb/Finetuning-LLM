SELECT
    customers.city,
    customers.is_active,
    MAX(subscriptions.start_date) AS last_start_date,
    MIN(subscriptions.start_date) AS min_start_date
FROM subscriptions
JOIN customers ON customers.id = subscriptions.customer_id
WHERE subscriptions.start_date BETWEEN '2023-04-27' AND '2025-04-01'
GROUP BY customers.city, customers.is_active
ORDER BY last_start_date, city, is_active
LIMIT 5;
