WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('month', shipped_date) shipped_date_period,
        DATEDIFF(CURRENT_DATE(), delivered_date) days_since_delivered_date,
        COALESCE(carrier, 'DIRECT') carrier_filled
    FROM shipments
    WHERE shipped_date > DATE '2023-03-09'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(days_since_delivered_date) OVER (PARTITION BY carrier, status ORDER BY shipped_date_period, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) days_since_delivered_date_avg_5_prior,
        AVG(days_since_delivered_date) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) days_since_delivered_date_avg_2,
        COUNT(days_since_delivered_date) OVER (ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) days_since_delivered_date_count_30d,
        LEAD(cost, 2, 0) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) next_cost
    FROM filtered
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY days_since_delivered_date_avg_2 DESC, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.status AS order_status,
        b.shipping_fee,
        b.discount
    FROM deduped e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN shipped_date_period - LAG(shipped_date_period) OVER (PARTITION BY status ORDER BY shipped_date_period, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
sessions_shipments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(shipped_date_period) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) AS prev_shipped_date_period
    FROM sessions_shipments
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_shipped_date_period IS NULL OR shipped_date_period > prev_shipped_date_period + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag_2
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipped_date_period,
        days_since_delivered_date,
        carrier_filled,
        days_since_delivered_date_avg_5_prior,
        days_since_delivered_date_avg_2,
        days_since_delivered_date_count_30d,
        next_cost,
        order_status,
        shipping_fee,
        discount,
        session_start_flag,
        burst_id,
        prev_shipped_date_period,
        session_start_flag_2,
        burst_id_2,
        AVG(days_since_delivered_date_avg_2) OVER (PARTITION BY order_status) order_status_avg_days_since_delivered_date_avg_2,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) cost_share
    FROM sessionized
)
SELECT
    id,
    carrier,
    delivered_date,
    cost,
    days_since_delivered_date_avg_5_prior,
    days_since_delivered_date_avg_2,
    days_since_delivered_date_count_30d,
    next_cost,
    order_status
FROM with_windows
ORDER BY cost NULLS LAST
