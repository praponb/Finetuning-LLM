SELECT a.id, a.customer_id, c.credit_limit, c.segment FROM accounts a FULL OUTER JOIN customers c ON a.customer_id = c.id WHERE UPPER(TRIM(c.email)) = 'HENRY@STARTUP.IO';
