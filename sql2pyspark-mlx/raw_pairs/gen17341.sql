SELECT
    id,
    balance,
    opened_date,
    LOWER(account_type) AS account_type_lower
FROM accounts
WHERE balance >= 76000 AND status = 'CLOSED';
