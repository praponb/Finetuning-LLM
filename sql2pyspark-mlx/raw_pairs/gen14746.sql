SELECT
    id,
    opened_date,
    status,
    account_type,
    CONCAT(account_type, ' - ', status) AS account_type_status
FROM accounts
WHERE status = 'FROZEN' OR account_type = 'LOAN';
