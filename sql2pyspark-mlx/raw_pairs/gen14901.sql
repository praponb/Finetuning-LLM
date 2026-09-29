SELECT 'customer' AS kind, signup_date AS event_date
FROM customers
WHERE signup_date IS NOT NULL
UNION ALL
SELECT 'account' AS kind, opened_date AS event_date
FROM accounts
WHERE opened_date IS NOT NULL
ORDER BY event_date DESC
