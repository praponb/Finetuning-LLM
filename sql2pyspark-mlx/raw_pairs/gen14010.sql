WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('MONTH', order_date) AS order_date_period
    FROM orders
    WHERE order_date > TO_DATE('2023-05-13')
      AND customer_id IN (14, 35)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.order_date_period >= sub.start_date
       AND s.order_date_period < sub.end_date + INTERVAL '30' DAY
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id, subscription_id) AS dedup_rank
    FROM matched
    QUALIFY dedup_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN order_date_period - LAG(order_date_period) OVER (PARTITION BY customer_id ORDER BY order_date_period, id, subscription_id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
sessions_orders AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY order_date_period, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    -- Attach stores attributes
    SELECT e.*, p.opened_date
    FROM sessions_orders e
    INNER JOIN stores p
        ON e.store_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1730 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    CASE WHEN shipping_fee >= 31 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    MIN(order_date_period) AS started_at,
    (UNIX_TIMESTAMP(MAX(order_date_period)) - UNIX_TIMESTAMP(MIN(order_date_period))) / 60 AS duration_min,
    MIN(order_date_period) AS first_seen,
    COUNT_IF(plan = 'premium') AS premium_events,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS monthly_fee_per_mobile,
    MIN(order_date_period) AS first_order_date_period
FROM joined
GROUP BY CASE WHEN amount >= 1730 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN shipping_fee >= 31 THEN 'LARGE' ELSE 'SMALL' END;
