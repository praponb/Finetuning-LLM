SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM accounts
) src
PIVOT (
    MAX(balance) AS total,
    COUNT(1) AS cnt
    FOR status IN ('FROZEN' AS frozen, 'OPEN' AS open)
)
