SELECT
    id,
    account_type,
    balance,
    opened_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rn
    FROM accounts
    WHERE account_type IN ('CREDIT', 'LOAN', 'SAVINGS')
) AS ranked
WHERE rn = 1
