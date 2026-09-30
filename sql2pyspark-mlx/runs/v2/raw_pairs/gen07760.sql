SELECT
    id,
    account_type,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS row_num
FROM accounts
