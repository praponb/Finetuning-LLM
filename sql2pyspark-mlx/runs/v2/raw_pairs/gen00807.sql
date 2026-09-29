SELECT *, CONCAT(status, ' ', account_type) AS status_account_type, CEIL(balance) AS balance_whole
FROM accounts
WHERE balance > 47000 AND status = 'OPEN' AND customer_id IS NULL
