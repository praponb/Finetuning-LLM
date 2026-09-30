WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.shipping_fee,
        p.amount AS order_amount
    FROM payments src
    INNER JOIN orders p
        ON src.order_id = p.id
    WHERE p.shipping_fee BETWEEN 23 AND 37
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        shipping_fee,
        MAX(shipping_fee) OVER w_ordered AS running_max_shipping_fee,
        SUM(shipping_fee) OVER w_ordered AS running_sum_shipping_fee,
        AVG(shipping_fee) OVER w_ordered AS running_avg_shipping_fee,
        MIN(shipping_fee) OVER w_ordered AS running_min_shipping_fee,
        MAX(order_amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS order_amount_max_30d
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.status,
        b.order_date
    FROM enriched e
    INNER JOIN orders b
        ON e.order_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        shipping_fee,
        running_max_shipping_fee,
        running_min_shipping_fee,
        status,
        LAG(running_avg_shipping_fee) OVER (PARTITION BY method ORDER BY running_sum_shipping_fee, id) AS prev_running_avg_shipping_fee,
        LEAD(running_avg_shipping_fee) OVER (PARTITION BY method ORDER BY running_sum_shipping_fee, id) AS next_running_avg_shipping_fee,
        LEAD(running_min_shipping_fee) OVER (PARTITION BY method ORDER BY running_sum_shipping_fee, id) AS next_running_min_shipping_fee,
        LAG(running_min_shipping_fee) OVER (PARTITION BY method ORDER BY running_sum_shipping_fee, id) AS prev_running_min_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY running_sum_shipping_fee, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY running_max_shipping_fee) AS running_max_shipping_fee_dense_rank
    FROM with_dims_payments
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    src.status AS payment_status,
    COUNT(*) AS num_rows,
    MAX(src.row_seq) AS max_row_seq
FROM metrics src
INNER JOIN orders o ON src.order_id = o.id
GROUP BY o.channel, src.status
