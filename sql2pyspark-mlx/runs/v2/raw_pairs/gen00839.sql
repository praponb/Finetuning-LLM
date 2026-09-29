WITH filtered_accounts AS (
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE account_type = 'SAVINGS' AND status IN ('OPEN', 'CLOSED', 'FROZEN')
),
accounts_stats AS (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        status,
        COUNT(DISTINCT balance) AS unique_customers,
        ROUND((100 * SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_open
    FROM filtered_accounts
    GROUP BY DATE_TRUNC('MONTH', opened_date), status
)
SELECT
    opened_month,
    status,
    unique_customers,
    pct_open
FROM accounts_stats
