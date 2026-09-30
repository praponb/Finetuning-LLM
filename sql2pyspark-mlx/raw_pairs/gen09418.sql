WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        INITCAP(TRIM(method)) AS method_title,
        CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END AS is_promptpay,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-10-29 00:00:00'
      AND TO_DATE(paid_at) >= '2023-03-26'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS LAST, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(amount) AS amount_whole
    FROM ranked
    WHERE order_id IS NOT NULL
      AND order_id IS NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY is_promptpay NULLS FIRST, id) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount) OVER w_hist AS prev_amount,
        LEAD(amount) OVER w_hist AS next_amount,
        LAG(is_promptpay) OVER w_hist AS prev_is_promptpay,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM deduped
    WHERE amount_whole >= 72
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY is_promptpay, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        is_promptpay,
        prev_amount,
        prev_is_promptpay,
        LAG(next_amount, 2) OVER (PARTITION BY order_id ORDER BY seq_no, id) AS prev_next_amount,
        FIRST_VALUE(prev_is_promptpay, TRUE) OVER (PARTITION BY order_id ORDER BY prev_is_promptpay, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_is_promptpay
    FROM metrics
)
SELECT
    id,
    is_promptpay,
    prev_next_amount
FROM with_windows
ORDER BY id NULLS LAST;
