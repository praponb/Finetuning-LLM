SELECT
    account_type,
    status,
    SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END) loan_count,
    MIN(opened_date) min_opened_date,
    SUM(CASE WHEN account_type = 'LOAN' THEN balance ELSE 0 END) loan_balance
FROM accounts
WHERE balance >= 35000
GROUP BY account_type, status
HAVING SUM(CASE WHEN account_type = 'LOAN' THEN balance ELSE 0 END) > 35
