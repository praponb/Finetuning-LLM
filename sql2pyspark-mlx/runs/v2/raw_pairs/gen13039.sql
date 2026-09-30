SELECT id, segment, signup_date, NTILE(2) OVER (PARTITION BY segment ORDER BY signup_date, id) AS bucket FROM customers WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM';
