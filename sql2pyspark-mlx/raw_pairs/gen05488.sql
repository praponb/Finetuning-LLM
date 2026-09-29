WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(method, 'NONE') AS method_clean
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'FAILED')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount_filled) OVER (PARTITION BY order_id ORDER BY amount, id) AS prev_amount_filled,
        LEAD(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS event_seq
    FROM base
    WHERE order_id IN (14, 29, 32)
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, o.order_date
    FROM metrics s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date NULLS LAST, id) AS rank_in_group
        FROM joined
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        MAX(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d,
        SUM(amount_filled) OVER (PARTITION BY method ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_filled
    FROM ranked
)
SELECT
    id,
    order_id,
    method,
    status,
    amount_filled,
    method_clean,
    prev_amount_filled,
    next_amount,
    amount_max_90d
FROM with_windows
ORDER BY method DESC NULLS FIRST;
