SELECT
    id,
    balance,
    status,
    COALESCE(status, 'N/A') AS status_filled
FROM accounts
WHERE customer_id IS NOT NULL AND customer_id = 4
ORDER BY status_filled;
