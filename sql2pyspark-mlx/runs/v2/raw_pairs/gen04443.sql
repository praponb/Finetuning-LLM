SELECT country, 'customer' source FROM customers WHERE signup_date > '2025-05-01' UNION ALL SELECT country, 'session' source FROM sessions WHERE browser IN ('Edge', 'Chrome');
