SELECT
    id,
    account_type,
    opened_date,
    DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS dense_rnk
FROM accounts
