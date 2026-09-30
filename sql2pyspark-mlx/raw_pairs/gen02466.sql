WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        shipping_fee,
        UPPER(channel) AS channel_upper,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date > TO_DATE('2023-06-15')
      AND store_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_amount,
        LEAD(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_shipping_fee,
        LAG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS row_seq
    FROM cleaned
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.order_date >= sub.start_date
       AND s.order_date < sub.end_date + INTERVAL 30 DAYS
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS seq_no,
        MAX(monthly_fee) OVER w_running AS customer_id_max_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC, id, subscription_id) AS event_seq,
        NTILE(3) OVER w_ordered AS subscription_id_bucket,
        SUM(shipping_fee) OVER (PARTITION BY status, store_id ORDER BY next_amount, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM attributed
    WHERE subscription_id = 5
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY prev_shipping_fee, id, subscription_id), w_running AS (PARTITION BY customer_id), w_ordered AS (PARTITION BY status, customer_id ORDER BY subscription_id DESC, id, subscription_id)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
        )
),
scored_orders AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id, subscription_id) AS shipping_fee_bucket,
        LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY customer_id_max_monthly_fee, id, subscription_id) AS prev_amount,
        SUM(amount) OVER (PARTITION BY DATE_TRUNC('YEAR', order_date), store_id ORDER BY next_amount, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        AVG(event_seq) OVER (PARTITION BY plan ORDER BY next_shipping_fee, id, subscription_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS event_seq_avg_5_prior
    FROM not_flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(customer_id_max_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS customer_id_max_monthly_fee_quartiles
FROM scored_orders
GROUP BY CASE WHEN seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END;
