WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.shipping_fee,
        b.discount
    FROM payments src
    INNER JOIN orders b
        ON src.order_id = b.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.amount AS order_amount,
        b.status AS order_status,
        b.order_date
    FROM with_dims src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM with_dims_payments
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    id,
    method,
    amount,
    paid_at,
    status,
    shipping_fee,
    discount,
    order_status,
    order_date,
    CASE WHEN method RLIKE '^(bank_transfer|promptpay)$' THEN 'CORE' WHEN method RLIKE '^(card|paypal)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS method_zone
FROM islands
ORDER BY amount DESC NULLS LAST;
