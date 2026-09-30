WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(paid_at) AS paid_at_date,
        CAST(burst_id AS STRING) AS burst_id_num,
        COALESCE(method, 'N/A') AS method_filled
    FROM sessionized
    WHERE order_id IS NOT NULL
      AND method NOT IN ('card', 'bank_transfer')
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.discount,
        p.amount AS order_amount,
        p.order_date
    FROM base a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.order_date AS order_order_date,
        d.channel
    FROM with_dims a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        burst_id,
        paid_at_date,
        burst_id_num,
        method_filled,
        discount,
        order_amount,
        order_order_date,
        channel,
        MIN(amount) OVER (PARTITION BY channel ORDER BY order_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        MAX(amount) OVER (PARTITION BY channel ORDER BY order_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        AVG(amount) OVER (PARTITION BY channel ORDER BY order_order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        AVG(discount) OVER (PARTITION BY MONTH(paid_at_date), channel ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5
    FROM joined
    WHERE status = 'SUCCESS'
)
-- Join a dimension and aggregate
SELECT
    d.channel AS order_channel,
    d.status AS order_status,
    src.channel AS payment_channel,
    COUNT(*) AS rows_total,
    MIN(src.running_avg_amount) AS min_running_avg_amount,
    MAX(src.running_min_amount) AS max_running_min_amount,
    GROUPING_ID() AS level_id
FROM metrics src
INNER JOIN orders d ON src.order_id = d.id
GROUP BY GROUPING SETS ((d.channel, d.status, src.channel), (d.channel, d.status), (d.channel), ())
