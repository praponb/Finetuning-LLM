WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', status, method) AS status_method_key,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2024-04-30 18:00:00'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        method,
        amount
    FROM filtered
) src
PIVOT (
    AVG(amount) AS amount
    FOR method IN ('cash', 'bank_transfer', 'paypal')
)
