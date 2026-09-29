WITH cleaned AS (
    SELECT
        *,
        QUARTER(opened_date) AS opened_date_quarter,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-03-30'
      AND customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        balance
    FROM cleaned
) src
PIVOT (
    MAX(balance) AS amount,
    COUNT(1) AS cnt
    FOR status IN ('CLOSED', 'FROZEN')
)
