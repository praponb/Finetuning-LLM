SELECT id, customer_id, TRIM(status) AS clean_status FROM accounts WHERE balance <= 52000 ORDER BY id DESC;
