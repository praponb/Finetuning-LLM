SELECT a.id, a.customer_id FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE UPPER(TRIM(email)) = 'ALICE@GMAIL.COM') ORDER BY a.id DESC;
