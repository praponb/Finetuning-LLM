SELECT *
FROM (
    SELECT
        customer_id,
        account_type,
        status,
        balance
    FROM accounts
) src
PIVOT (
    MAX(balance) AS total,
    COUNT(1) AS cnt
    FOR status IN ('CLOSED', 'OPEN')
)
