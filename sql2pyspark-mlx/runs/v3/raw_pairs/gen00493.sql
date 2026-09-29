SELECT
    id,
    status,
    account_type,
    opened_date,
    SUBSTRING(status, 1, 3) AS status_prefix,
    COALESCE(status, 'N/A') AS status_filled
FROM accounts
WHERE status NOT IN ('OPEN', 'CLOSED')
