SELECT id, opened_date, account_type, customer_id, INITCAP(TRIM(account_type)) AS account_type_title FROM accounts WHERE balance >= 73000 ORDER BY account_type_title, id LIMIT 3
