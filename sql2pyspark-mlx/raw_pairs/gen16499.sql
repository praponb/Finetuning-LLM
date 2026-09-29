SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.amount,
    s.discount,
    top1.payment_paid_at,
    top1.payment_status
FROM orders s
JOIN LATERAL (
        SELECT c.paid_at AS payment_paid_at, c.status AS payment_status
        FROM payments c
        WHERE c.order_id = s.id
          AND c.paid_at <= s.order_date
        ORDER BY c.paid_at DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
