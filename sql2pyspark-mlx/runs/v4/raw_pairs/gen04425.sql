WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        NTILE(5) OVER (PARTITION BY method, order_id ORDER BY amount, id) AS amount_bucket
    FROM payments
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        last_known_amount,
        amount_bucket,
        CEIL(last_known_amount) AS last_known_amount_whole
    FROM metrics
    WHERE paid_at > DATE '2024-03-19'
),
base_payments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        amount,
        last_known_amount,
        last_known_amount_whole,
        last_known_amount_whole - amount AS last_known_amount_whole_minus_amount,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM base
    WHERE method NOT IN ('promptpay', 'bank_transfer')
)
SELECT
    id,
    amount,
    last_known_amount_whole,
    last_known_amount_whole_minus_amount,
    resolved_method
FROM base_payments
ORDER BY id DESC, last_known_amount_whole;
