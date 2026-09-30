SELECT
    subscriptions.id,
    subscriptions.customer_id,
    subscriptions.status,
    subscriptions.start_date
FROM subscriptions
WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE segment = 'SMB')
ORDER BY subscriptions.id DESC, subscriptions.status ASC
