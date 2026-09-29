WITH filtered AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        CONCAT(method, '/', status) AS method_status
    FROM payments
    WHERE method IS NULL
)
SELECT *
FROM (
    SELECT
        order_id,
        method,
        amount
    FROM filtered
) src
PIVOT (
    MAX(amount) AS value_sum,
    COUNT(1) AS item_count
    FOR method IN ('card' AS card, 'paypal' AS paypal)
)
