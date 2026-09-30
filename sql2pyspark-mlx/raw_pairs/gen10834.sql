SELECT sessions.id, sessions.country, sessions.browser
FROM sessions
WHERE sessions.customer_id IN (SELECT id FROM customers WHERE country IN ('JP', 'SG', 'UK'))
ORDER BY browser DESC, country
