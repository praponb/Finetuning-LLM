WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-22 17:00:00'
      AND amount <= 880
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag_2
    FROM islands
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.shipping_fee,
        d.status AS order_status
    FROM sessionized src
    INNER JOIN orders d
        ON src.order_id <=> d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(session_id) OVER (PARTITION BY status ORDER BY amount_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS session_id_avg_6,
        LEAD(session_id_2, 2) OVER (PARTITION BY order_id, status ORDER BY shipping_fee, id) AS next_session_id_2,
        LAG(amount_filled, 1) OVER (PARTITION BY order_id, status ORDER BY session_id, id) AS prev_amount_filled
    FROM with_dims
    WHERE status <> 'REFUNDED'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', paid_at) AS year_start,
        COUNT(*) FILTER (WHERE order_status = 'RETURNED') AS returned_rows,
        MIN(amount) AS min_amount,
        SUM(session_id_avg_6) FILTER (WHERE status = 'SUCCESS') AS success_total_session_id_avg_6
    FROM with_windows
    GROUP BY DATE_TRUNC('YEAR', paid_at)
    HAVING min_amount < 1000000
)
SELECT year_start, success_total_session_id_avg_6
FROM stats;
