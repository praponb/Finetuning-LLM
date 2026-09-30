SELECT
    id,
    balance,
    account_type,
    status,
    SUBSTRING(account_type, 1, 4) AS account_type_prefix
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CREDIT', 'CHECKING') AND (balance <= 61000 OR opened_date BETWEEN '2023-10-21' AND '2025-09-01')
ORDER BY account_type_prefix, id
