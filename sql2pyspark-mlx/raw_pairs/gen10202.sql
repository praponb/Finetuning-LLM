SELECT
    c.id,
    c.city,
    c.segment,
    c.name
FROM customers c
WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee <= 190)
ORDER BY id, c.segment DESC;
