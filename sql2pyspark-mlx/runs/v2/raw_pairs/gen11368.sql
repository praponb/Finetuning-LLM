SELECT credit_limit FROM customers WHERE email IS NOT NULL AND email LIKE '%@startup.io' ORDER BY credit_limit DESC;
