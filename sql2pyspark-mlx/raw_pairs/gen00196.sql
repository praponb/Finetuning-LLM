WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('WEEK', paid_at) AS paid_at_period,
        CASE WHEN method IN ('paypal', 'bank_transfer') THEN 'Group A' ELSE 'Other' END AS method_group
    FROM payments
    WHERE status = 'SUCCESS'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.amount,
        a.paid_at,
        a.method_group,
        b.amount AS order_amount
    FROM base a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(order_amount) OVER w_grp AS order_amount_count_7d,
        NTILE(10) OVER (PARTITION BY order_id, method ORDER BY amount, id) AS amount_bucket,
        FIRST_VALUE(amount) IGNORE NULLS OVER (ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        SUM(order_amount) OVER w_recent AS cumulative_order_amount
    FROM with_dims
    WHERE order_amount BETWEEN 510 AND 1020
    WINDOW w_grp AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(is_new_burst) OVER w_part AS prev_is_new_burst,
        LEAD(is_new_burst) OVER w_part AS next_is_new_burst,
        LAG(amount_bucket) OVER w_part AS prev_amount_bucket,
        LEAD(amount_bucket) OVER w_part AS next_amount_bucket,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM sessionized
    WINDOW w_part AS (PARTITION BY order_id ORDER BY cumulative_order_amount, id)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics_payments
    QUALIFY RANK() OVER (PARTITION BY method ORDER BY event_seq) = 1
)
SELECT
    id,
    method,
    amount,
    paid_at,
    method_group,
    order_amount_count_7d,
    first_known_amount,
    cumulative_order_amount,
    island_id
FROM latest
