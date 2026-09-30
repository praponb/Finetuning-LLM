WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(order_date, 14) AS order_date_minus_14d,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM orders
    WHERE channel IS DISTINCT FROM 'store'
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM cleaned
    EXCEPT ALL
    SELECT customer_id, status
    FROM orders
    WHERE customer_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_minus_14d,
        channel_filled,
        COUNT(shipping_fee) OVER (PARTITION BY MONTH(order_date_minus_14d), channel) AS order_date_minus_14d_month_count_shipping_fee,
        CUME_DIST() OVER w_recent AS discount_cume_dist,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 4) AS amount_share
    FROM verified
    WHERE channel IN ('store', 'mobile')
    WINDOW w_recent AS (ORDER BY discount DESC NULLS FIRST)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at < s.order_date
        )
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT customer_id, id
    FROM orders
    WHERE amount >= 1380
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN kept_keys_2 k ON s.customer_id = k.customer_id AND s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN order_date_minus_14d_month_count_shipping_fee >= 16 THEN 'LARGE' ELSE 'SMALL' END AS order_date_minus_14d_month_count_shipping_fee_tier,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        COUNT_IF(channel = 'mobile') AS mobile_events,
        ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS shipping_fee_per_store,
        STDDEV_SAMP(discount) AS discount_stddev,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        GROUPING_ID() AS grouping_level
    FROM reconciled
    GROUP BY CUBE (CASE WHEN order_date_minus_14d_month_count_shipping_fee >= 16 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', order_date))
)
SELECT
    order_date_minus_14d_month_count_shipping_fee_tier,
    discount_stddev,
    channel_list
FROM daily
ORDER BY discount_stddev
