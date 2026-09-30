SELECT country FROM customers UNION SELECT country FROM sessions WHERE browser NOT IN ('Safari', 'Chrome', 'Edge');
