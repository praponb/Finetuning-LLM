SELECT
    id,
    opened_date,
    status,
    account_type,
    DAYOFWEEK(opened_date) opened_date_dayofweek
FROM accounts
WHERE status = 'FROZEN'
ORDER BY account_type, opened_date DESC;
