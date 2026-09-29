WITH cleaned AS (
    SELECT
        *,
        UPPER(account_type) account_type_upper,
        CONCAT(status, ', ', account_type) status_account_type,
        COALESCE(status, 'DIRECT') resolved_status
    FROM accounts
    WHERE opened_date >= '2025-10-01'
)
SELECT status, id
FROM cleaned
INTERSECT
SELECT status, id
FROM accounts
WHERE balance BETWEEN 52000 AND 56000;
