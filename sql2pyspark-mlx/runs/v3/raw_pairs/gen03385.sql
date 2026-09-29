WITH Src AS (
    SELECT *
    FROM accounts
    WHERE balance < 32000 AND status IN ('FROZEN', 'OPEN')
),
summary AS (
    SELECT
        status,
        account_type,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
        COUNT(DISTINCT balance) AS unique_account_types
    FROM Src
    GROUP BY status, account_type
)
SELECT *
FROM summary
ORDER BY unique_account_types DESC
