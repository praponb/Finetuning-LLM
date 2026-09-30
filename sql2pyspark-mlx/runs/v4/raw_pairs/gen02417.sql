SELECT *
FROM (
    SELECT
        account_type,
        customer_id,
        status,
        balance
    FROM accounts
) src
PIVOT (
    SUM(balance) AS total
    FOR status IN ('FROZEN', 'CLOSED')
)
