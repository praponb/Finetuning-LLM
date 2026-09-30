WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        INITCAP(TRIM(status)) AS status_title
    FROM accounts
    WHERE status IS NOT NULL
      AND status IS DISTINCT FROM 'FROZEN'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM filtered
) src
PIVOT (
    MAX(balance) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('FROZEN', 'OPEN')
);
