SELECT sessions.id, sessions.device, sessions.country
FROM sessions
WHERE sessions.device IN ('desktop', 'mobile') AND sessions.customer_id NOT IN (SELECT id FROM customers WHERE signup_date <= '2023-08-17');
