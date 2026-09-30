SELECT
    id,
    name,
    email,
    CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END is_consumer,
    LENGTH(email) email_length
FROM customers
WHERE segment = 'Consumer'
