SELECT
    id,
    status,
    balance,
    opened_date,
    COALESCE(status, 'unassigned') status_filled
FROM accounts
