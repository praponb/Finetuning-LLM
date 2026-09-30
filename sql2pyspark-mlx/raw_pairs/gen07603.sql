WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', account_type, status) AS account_type_status_key,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'OPEN'
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    COUNT(*) AS cnt
FROM valid
GROUP BY DATE_TRUNC('YEAR', opened_date)
HAVING num_account_types < 1000
ORDER BY year_start NULLS FIRST;
