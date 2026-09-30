WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    SELECT e.*, d.channel
    FROM sessions_payments e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.order_date > '2024-11-01'
),
gaps_2 AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
metrics AS (
    SELECT
        *,
        SUM(session_no) OVER w_running AS running_sum_session_no,
        COUNT(session_no) OVER w_running AS running_count_session_no,
        AVG(session_no) OVER w_running AS running_avg_session_no,
        FIRST_VALUE(is_new_burst) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_burst
    FROM sessionized
    WINDOW w_running AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT e.*, p.discount
    FROM metrics e
    INNER JOIN orders p
        ON e.order_id = p.id
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM joined
),
gaps_3 AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_payments_2 AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps_3
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.session_no,
    s.channel,
    s.is_new_session,
    s.running_sum_session_no,
    (SELECT AVG(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS avg_payment_amount
FROM sessions_payments_2 s;
