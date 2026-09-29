SELECT customers.country, MAX(subscriptions.end_date) AS latest_end_date, MIN(subscriptions.start_date) AS first_start_date
FROM subscriptions
INNER JOIN customers ON subscriptions.customer_id = customers.id
GROUP BY customers.country;
