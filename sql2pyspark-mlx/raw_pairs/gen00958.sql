WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status ORDER BY amount DESC, id ASC) AS amount_bucket,
        LAG(amount, 1, 0) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS prev_amount,
        MAX(amount) OVER (PARTITION BY method) AS method_max_amount,
        AVG(amount) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        AVG(amount) OVER w_part AS amount_avg_6_prior
    FROM payments
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('MONTH', paid_at), method ORDER BY paid_at ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.amount_bucket,
    s.prev_amount,
    s.method_max_amount,
    s.amount_avg_3,
    (SELECT AVG(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS avg_payment_amount
FROM with_windows s
