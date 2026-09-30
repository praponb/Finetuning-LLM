WITH valid AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        CONCAT_WS(',', status, method) AS status_method_key,
        ROUND(amount * 1.1, 2) AS amount_scaled,
        COALESCE(method, 'NONE') AS method_clean
    FROM payments
    WHERE paid_at > DATE '2023-10-08'
)
SELECT *
FROM (
    SELECT
        order_id,
        method,
        amount
    FROM valid
) src
PIVOT (
    AVG(amount)
    FOR method IN ('cash' AS cash, 'paypal' AS paypal, 'promptpay' AS promptpay)
)
