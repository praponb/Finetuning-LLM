WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(amount AS INT) amount_int,
        DATE_TRUNC('week', order_date) order_date_period
    FROM orders
    WHERE customer_id IS NOT NULL
      AND order_date > DATE '2023-07-24'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(discount) OVER (PARTITION BY customer_id ORDER BY amount, id) prev_discount,
        LAG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY amount, id) prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount, id) event_seq,
        LAST_VALUE(discount, TRUE) OVER (PARTITION BY status ORDER BY amount_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_discount
    FROM valid
    WHERE status = 'COMPLETED'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(order_date_period) OVER (PARTITION BY status ORDER BY order_date_period, id) AS prev_order_date_period
    FROM metrics
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_order_date_period IS NULL OR order_date_period > prev_order_date_period + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_orders AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_period,
        prev_discount,
        event_seq,
        last_known_discount,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.unit_price,
        latest.order_item_quantity
    FROM sessions_orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        discount,
        shipping_fee,
        event_seq,
        AVG(amount) OVER (PARTITION BY customer_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) amount_avg_5,
        AVG(amount) OVER (PARTITION BY status ORDER BY CAST(order_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_avg_7d,
        ROW_NUMBER() OVER w_part row_seq
    FROM with_last
    WINDOW w_part AS (PARTITION BY status, channel ORDER BY order_item_quantity DESC, id)
)
-- Grouped metrics
SELECT
    CASE WHEN shipping_fee >= 38 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    APPROX_PERCENTILE(shipping_fee, 0.9) AS shipping_fee_p90,
    ROUND(AVG(row_seq), 2) AS avg_row_seq
FROM with_windows
GROUP BY CASE WHEN shipping_fee >= 38 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY shipping_fee_p90 DESC
