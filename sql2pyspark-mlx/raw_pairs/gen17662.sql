SELECT *, COALESCE(status, 'Unknown') AS status_filled, CONCAT(account_type, ', ', status) AS account_type_status FROM accounts
