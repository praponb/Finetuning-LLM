SELECT
    id,
    status,
    account_type,
    CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END AS is_savings
FROM accounts
WHERE account_type NOT IN ('CHECKING', 'LOAN', 'SAVINGS') AND account_type <> 'LOAN'
