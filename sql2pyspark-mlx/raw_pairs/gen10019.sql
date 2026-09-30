WITH valid AS (
    SELECT
        id,
        order_id,
        delivered_date,
        DATE_TRUNC('MONTH', shipped_date) AS shipped_date_period,
        SUBSTRING(carrier, 1, 4) AS carrier_prefix
    FROM shipments
    WHERE order_id IN (33, 34, 40)
      AND shipped_date > DATE '2023-02-20'
),
with_windows AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) AS seq_no
    FROM valid
    WHERE order_id IS NOT NULL
),
prev_events AS (
    SELECT *, LAG(shipped_date_period) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) AS prev_shipped_date_period
    FROM with_windows
),
flagged AS (
    SELECT *, CASE WHEN prev_shipped_date_period IS NULL OR shipped_date_period > prev_shipped_date_period + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
prev_events_2 AS (
    SELECT *, LAG(shipped_date_period) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) AS prev_shipped_date_period_2
    FROM sessionized
),
with_gaps AS (
    SELECT *, CASE WHEN prev_shipped_date_period_2 IS NULL OR shipped_date_period > prev_shipped_date_period_2 + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events_2
),
sessionized_shipments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
kept_keys AS (
    SELECT order_id, id
    FROM sessionized_shipments
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE shipped_date <= '2024-01-01'
),
verified AS (
    SELECT s.*
    FROM sessionized_shipments s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
scored AS (
    SELECT
        *,
        MAX(seq_no) OVER (PARTITION BY order_id) AS order_id_max_seq_no,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY seq_no) AS seq_no_dense_rank,
        SUM(session_no) OVER (PARTITION BY MONTH(delivered_date), order_id ORDER BY CAST(prev_shipped_date_period_2 AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_no_sum_7d
    FROM verified
),
with_dims AS (
    SELECT
        a.*,
        b.discount,
        b.amount
    FROM scored a
    INNER JOIN orders b
        ON a.order_id = b.id
)
SELECT
    DATE_TRUNC('QUARTER', prev_shipped_date_period) AS quarter_start,
    CASE WHEN session_start_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS session_start_flag_tier,
    island_id,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(prev_shipped_date_period) AS last_seen,
    COUNT(*) AS cnt
FROM with_dims
GROUP BY DATE_TRUNC('QUARTER', prev_shipped_date_period), CASE WHEN session_start_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END, island_id
HAVING distinct_ids < 1000
ORDER BY quarter_start DESC NULLS FIRST
