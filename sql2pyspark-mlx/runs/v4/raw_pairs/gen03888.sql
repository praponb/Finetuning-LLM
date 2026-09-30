SELECT *, CONCAT(account_type, ' ', status) AS account_type_status, ABS(balance) AS abs_balance FROM accounts ORDER BY opened_date, id LIMIT 10;
