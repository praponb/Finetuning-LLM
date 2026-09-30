SELECT
    id,
    balance,
    opened_date,
    FIRST_VALUE(opened_date) OVER (ORDER BY balance DESC, id) AS first_opened_date
FROM accounts;
