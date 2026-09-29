WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        shipping_fee,
        CASE WHEN shipping_fee >= 32 THEN 'High' WHEN shipping_fee >= 8 THEN 'Medium' ELSE 'Low' END AS shipping_fee_band
    FROM orders
    WHERE order_date > DATE '2023-03-31'
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        shipping_fee,
        shipping_fee_band,
        LEAD(shipping_fee, 1) OVER w_running AS next_shipping_fee,
        LAST_VALUE(shipping_fee, TRUE) OVER (ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        MIN(shipping_fee) OVER (PARTITION BY status) AS status_min_shipping_fee,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id ASC) AS seq_no
    FROM valid
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY shipping_fee ASC, id ASC)
),
with_windows_orders AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY shipping_fee DESC) AS shipping_fee_dense_rank,
        SUM(last_known_shipping_fee) OVER (ORDER BY first_known_shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_shipping_fee
    FROM with_windows
    WHERE last_known_shipping_fee IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.next_shipping_fee,
        e.last_known_shipping_fee,
        e.status_min_shipping_fee,
        e.first_known_shipping_fee,
        e.cumulative_last_known_shipping_fee,
        c.city,
        c.name
    FROM with_windows_orders e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
)
-- Window metrics over several different partitions
SELECT
    *,
    AVG(status_min_shipping_fee) OVER w_part AS running_avg_status_min_shipping_fee,
    MIN(status_min_shipping_fee) OVER w_part AS running_min_status_min_shipping_fee,
    MAX(status_min_shipping_fee) OVER w_part AS running_max_status_min_shipping_fee,
    SUM(status_min_shipping_fee) OVER w_part AS running_sum_status_min_shipping_fee
FROM with_dims
WINDOW w_part AS (PARTITION BY customer_id ORDER BY first_known_shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
