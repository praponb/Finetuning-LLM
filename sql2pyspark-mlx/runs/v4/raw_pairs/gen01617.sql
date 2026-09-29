SELECT COUNT(DISTINCT amount) AS unique_accounts, MAX(amount) - MIN(amount) AS amount_range FROM transactions WHERE txn_time < '2024-12-09' ORDER BY unique_accounts
