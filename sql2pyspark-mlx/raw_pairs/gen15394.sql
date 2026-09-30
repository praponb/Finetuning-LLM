WITH flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM sessionized
),
flagged_2 AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM prev_events
),
sessionized_payments AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
cleaned AS (
    SELECT *, HOUR(paid_at) AS paid_at_hour
    FROM sessionized_payments
    WHERE order_id IS NOT NULL
),
joined AS (
    SELECT
        a.*,
        o.order_date,
        o.channel,
        o.amount AS order_amount
    FROM cleaned a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
totals AS (
    SELECT
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
        MIN(amount) AS min_amount,
        SUM(CASE WHEN channel = 'mobile' THEN island_id ELSE 0 END) AS mobile_island_id
    FROM joined
    GROUP BY 1
    HAVING SUM(CASE WHEN channel = 'mobile' THEN island_id ELSE 0 END) < 1000000
)
SELECT activity_month, mobile_rows
FROM totals
ORDER BY mobile_rows ASC NULLS FIRST
