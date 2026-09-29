WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        TRIM(account_type) AS clean_account_type,
        DATE_ADD(opened_date, 3) AS opened_date_plus_3d
    FROM accounts
    WHERE balance BETWEEN 24000 AND 67000
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    status,
    COUNT_IF(account_type = 'CREDIT') AS credit_events,
    COUNT_IF(account_type = 'SAVINGS') AS savings_events,
    COUNT(DISTINCT status) AS distinct_status
FROM base
GROUP BY DATE_TRUNC('MONTH', opened_date), status;
