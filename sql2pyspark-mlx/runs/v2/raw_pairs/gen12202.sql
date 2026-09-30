SELECT
    status,
    balance,
    LOWER(status) status_lower,
    COALESCE(status, 'Unknown') status_filled
FROM accounts
WHERE status IS NULL
