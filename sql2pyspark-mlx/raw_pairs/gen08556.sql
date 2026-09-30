WITH with_dims AS (
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        p.order_date,
        p.amount
    FROM order_items src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
metrics AS (
    SELECT
        id,
        product_id,
        order_date,
        STDDEV_SAMP(amount) OVER (PARTITION BY MONTH(order_date), product_id) AS order_date_month_stddev_samp_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY MONTH(order_date), order_id), 4) AS amount_share
    FROM with_dims
),
scored AS (
    SELECT
        id,
        order_date_month_stddev_samp_amount,
        amount_share,
        MIN(order_date_month_stddev_samp_amount) OVER w_ordered AS last4_min_order_date_month_stddev_samp_amount,
        COUNT(order_date_month_stddev_samp_amount) OVER w_ordered AS last4_count_order_date_month_stddev_samp_amount,
        AVG(order_date_month_stddev_samp_amount) OVER w_ordered AS last4_avg_order_date_month_stddev_samp_amount,
        SUM(order_date_month_stddev_samp_amount) OVER w_ordered AS last4_sum_order_date_month_stddev_samp_amount
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY order_date_month_stddev_samp_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        amount_share,
        last4_avg_order_date_month_stddev_samp_amount,
        SUM(amount_share) OVER (ORDER BY order_date_month_stddev_samp_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_share,
        AVG(amount_share) OVER (ORDER BY order_date_month_stddev_samp_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_share_avg_2,
        ROW_NUMBER() OVER (ORDER BY order_date_month_stddev_samp_amount DESC, id) AS row_seq,
        AVG(order_date_month_stddev_samp_amount) OVER (ORDER BY amount_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS order_date_month_stddev_samp_amount_avg_2,
        LAG(last4_sum_order_date_month_stddev_samp_amount, 1) OVER (ORDER BY last4_count_order_date_month_stddev_samp_amount, id) AS prev_last4_sum_order_date_month_stddev_samp_amount
    FROM scored
)
SELECT
    id,
    amount_share,
    cumulative_amount_share,
    amount_share_avg_2,
    row_seq,
    order_date_month_stddev_samp_amount_avg_2,
    prev_last4_sum_order_date_month_stddev_samp_amount
FROM with_windows;
