SELECT *
FROM (
    SELECT
        method,
        order_id,
        status,
        amount
    FROM payments
) src
PIVOT (
    MAX(amount)
    FOR status IN ('SUCCESS', 'FAILED')
)
