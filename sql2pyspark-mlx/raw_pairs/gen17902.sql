WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CONCAT_WS('-', method, status) AS method_status_key,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE status NOT IN ('REFUNDED', 'SUCCESS')
      AND method IS DISTINCT FROM 'promptpay'
),
cleaned_payments AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(method) AS method_lower
    FROM cleaned
    WHERE order_id IS NOT NULL
      AND paid_at >= DATE '2023-05-20'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        method_filled,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY order_id, method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        FIRST_VALUE(method, TRUE) OVER w_hist AS first_known_method
    FROM cleaned_payments
    WHERE order_id IN (13, 22, 27, 30)
    WINDOW w_hist AS (PARTITION BY status, method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        first_known_method,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    method,
    paid_at,
    first_known_method,
    session_id,
    CASE WHEN method RLIKE '^(cash|paypal)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS method_zone
FROM sessions_payments
ORDER BY session_id DESC;
