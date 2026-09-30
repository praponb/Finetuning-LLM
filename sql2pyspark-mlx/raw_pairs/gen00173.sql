WITH with_windows AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY MONTH(paid_at), method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        NTILE(3) OVER (ORDER BY amount DESC, id ASC) AS amount_bucket,
        LAG(amount, 1) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) AS prev_amount
    FROM payments
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.amount_bucket,
    s.prev_amount,
    (SELECT SUM(x.quantity) FROM order_items x WHERE x.order_id = s.order_id) AS sum_order_item_quantity
FROM with_windows s
ORDER BY status DESC NULLS LAST;
