WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        DATE_ADD(opened_date, 62) opened_date_plus_62d,
        COALESCE(status, 'UNKNOWN') resolved_status
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND opened_date > TO_DATE('2023-05-29')
)
-- Attach customers attributes
SELECT
    s.id,
    s.customer_id,
    s.balance,
    b.city
FROM filtered s
LEFT JOIN customers b
    ON s.customer_id <=> b.id
WHERE b.age > 36
