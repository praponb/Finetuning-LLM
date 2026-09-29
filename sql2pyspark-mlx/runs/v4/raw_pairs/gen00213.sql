WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('MONTH', delivered_date) AS delivered_date_period
    FROM shipments
    WHERE delivered_date > DATE '2023-06-18'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.status AS order_status,
        d.amount
    FROM filtered a
    INNER JOIN orders d
        ON a.order_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        QUARTER(shipped_date) AS shipped_date_quarter,
        ROUND(amount, 1) AS amount_rounded
    FROM with_dims
    WHERE shipped_date >= TO_DATE('2023-12-16')
      AND order_id IN (8, 28, 32, 35)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN delivered_date_period - LAG(delivered_date_period) OVER (PARTITION BY order_status ORDER BY delivered_date_period, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessions_shipments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_status ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(island_id) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS island_id_avg_3,
        AVG(island_id) OVER (PARTITION BY order_status ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS island_id_avg_2_prior,
        NTILE(5) OVER (PARTITION BY carrier, order_id ORDER BY amount, id) AS amount_bucket
    FROM sessions_shipments
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    CASE WHEN amount >= 1560 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    (UNIX_TIMESTAMP(MAX(delivered_date_period)) - UNIX_TIMESTAMP(MIN(delivered_date_period))) / 60 AS duration_min,
    MIN(delivered_date_period) AS first_delivered_date_period,
    (UNIX_TIMESTAMP(MAX(delivered_date_period)) - UNIX_TIMESTAMP(MIN(delivered_date_period))) / 60 AS duration_min_2,
    SIZE(COLLECT_SET(order_status)) AS num_order_status
FROM metrics
GROUP BY order_id, CASE WHEN amount >= 1560 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY duration_min_2 NULLS FIRST;
