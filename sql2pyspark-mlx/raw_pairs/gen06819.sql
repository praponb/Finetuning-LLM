WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        CONCAT(status, ', ', channel) AS status_channel,
        CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled
    FROM orders
    WHERE store_id IS NOT NULL
      AND discount IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_cancelled) OVER (PARTITION BY status, customer_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_is_cancelled,
        COUNT(is_cancelled) OVER (PARTITION BY status, customer_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_cancelled,
        MIN(is_cancelled) OVER (PARTITION BY status, customer_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_cancelled
    FROM filtered
    WHERE shipping_fee >= 25
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        discount,
        shipping_fee,
        status_channel,
        running_avg_is_cancelled,
        running_count_is_cancelled,
        LEAD(running_min_is_cancelled) OVER w_hist AS next_running_min_is_cancelled,
        LAG(shipping_fee) OVER w_hist AS prev_shipping_fee,
        LEAD(shipping_fee) OVER w_hist AS next_shipping_fee,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM metrics
    WINDOW w_hist AS (PARTITION BY status, customer_id ORDER BY running_avg_is_cancelled ASC, id ASC)
),
with_windows_orders AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        discount,
        shipping_fee,
        status_channel,
        next_running_min_is_cancelled,
        next_shipping_fee,
        seq_no,
        MIN(next_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_count_is_cancelled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_shipping_fee,
        MAX(next_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_count_is_cancelled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_next_shipping_fee,
        COUNT(next_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_count_is_cancelled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_shipping_fee,
        AVG(next_shipping_fee) OVER (PARTITION BY customer_id ORDER BY running_count_is_cancelled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_shipping_fee,
        STDDEV_SAMP(prev_shipping_fee) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_prev_shipping_fee
    FROM with_windows
)
-- Join a dimension and aggregate
SELECT
    d.country,
    d.segment,
    COUNT(*) AS cnt,
    SUM(src.next_running_min_is_cancelled) AS total_next_running_min_is_cancelled,
    GROUPING_ID() AS grouping_level
FROM with_windows_orders src
INNER JOIN customers d ON src.customer_id = d.id
GROUP BY ROLLUP (d.country, d.segment)
