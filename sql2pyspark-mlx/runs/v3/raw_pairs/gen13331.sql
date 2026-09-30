SELECT id, balance, customer_id, status, CONCAT(status, ' ', account_type) AS status_account_type, CAST(balance AS INT) AS balance_int FROM accounts WHERE YEAR(opened_date) = 2025;
