SELECT
    id,
    status,
    balance,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS balance_row,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY opened_date DESC) AS dense_rnk
FROM accounts
ORDER BY opened_date DESC;
