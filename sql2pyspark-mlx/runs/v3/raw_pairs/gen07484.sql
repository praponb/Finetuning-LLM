SELECT
    id,
    status,
    balance,
    opened_date,
    FIRST_VALUE(opened_date) OVER (PARTITION BY status ORDER BY balance DESC, id) AS first_opened_date
FROM accounts;
