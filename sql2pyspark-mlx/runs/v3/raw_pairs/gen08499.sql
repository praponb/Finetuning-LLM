SELECT
    id,
    account_type,
    balance,
    status
FROM accounts
WHERE YEAR(opened_date) >= 2023
ORDER BY account_type DESC, status ASC;
