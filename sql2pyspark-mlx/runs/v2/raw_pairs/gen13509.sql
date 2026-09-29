SELECT country, 'customer' AS source FROM customers WHERE credit_limit BETWEEN 4500 AND 13200 UNION ALL SELECT country, 'session' AS source FROM sessions WHERE browser IN ('Edge', 'Safari');
