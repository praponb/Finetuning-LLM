SELECT
    id,
    status,
    account_type,
    balance,
    PERCENT_RANK() OVER (PARTITION BY status, account_type ORDER BY balance DESC) AS balance_pct_rank
FROM accounts
