WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.channel,
        b.shipping_fee
    FROM payments a
    LEFT JOIN orders b
        ON a.order_id = b.id
    WHERE b.channel = 'web'
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.amount,
        src.status,
        src.shipping_fee,
        o.amount AS order_amount
    FROM with_dims src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.shipping_fee,
        src.order_amount,
        o.channel,
        o.discount
    FROM with_dims_payments src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel, order_id ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_shipping_fee,
        NTILE(5) OVER (ORDER BY discount, id) discount_bucket,
        SUM(amount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount
    FROM enriched
),
enriched_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipping_fee,
        discount_bucket,
        cumulative_amount,
        SUM(discount) OVER w_hist running_sum_discount,
        MAX(discount) OVER w_hist running_max_discount,
        AVG(discount) OVER w_hist running_avg_discount,
        COUNT(discount_bucket) OVER (PARTITION BY order_id) order_id_count_discount_bucket
    FROM enriched_payments
    WINDOW w_hist AS (PARTITION BY order_id, channel ORDER BY discount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipping_fee,
        discount_bucket,
        cumulative_amount,
        running_sum_discount,
        running_avg_discount,
        order_id_count_discount_bucket,
        LAST_VALUE(order_id_count_discount_bucket) IGNORE NULLS OVER (ORDER BY running_max_discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_order_id_count_discount_bucket,
        LAG(cumulative_amount, 1) OVER (ORDER BY running_avg_discount, id) prev_cumulative_amount
    FROM enriched_payments_2
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        running_sum_discount,
        shipping_fee,
        running_avg_discount
    FROM with_windows
) src
UNPIVOT (
    value FOR metric_name IN (running_sum_discount AS RUNNING_SUM_DISCOUNT_VAL, shipping_fee AS SHIPPING_FEE_VAL, running_avg_discount AS RUNNING_AVG_DISCOUNT)
);
