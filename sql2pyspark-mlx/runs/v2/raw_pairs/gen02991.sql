SELECT id, customer_id, opened_date, INITCAP(TRIM(account_type)) AS account_type_title FROM accounts WHERE account_type = 'CREDIT' AND MONTH(opened_date) IN (1, 5, 7);
