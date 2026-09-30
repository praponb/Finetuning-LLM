SELECT country FROM customers UNION ALL SELECT country FROM sessions WHERE browser IN ('Chrome', 'Firefox', 'Edge');
