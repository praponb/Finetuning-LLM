SELECT
    id,
    balance,
    account_type,
    status,
    COALESCE(status, 'unassigned') AS status_filled
FROM accounts;
